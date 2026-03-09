from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text


def _colunas_tabela(engine, tabela):
    inspector = inspect(engine)
    return {c["name"].lower() for c in inspector.get_columns(tabela, schema="dbo")}


def _resolver_coluna_data(colunas):
    if "datalan" in colunas:
        return "DataLan"
    if "data" in colunas:
        return "[data]"
    raise RuntimeError("A dbo.T_MOVEST nao possui coluna de data valida para auditoria.")


def _resolver_expr_seqit(colunas):
    if "seqit" in colunas:
        return "TRY_CAST(SEQIT AS BIGINT)"
    if "registro" in colunas:
        return "TRY_CAST(Registro AS BIGINT)"
    return "CAST(NULL AS BIGINT)"


def _resolver_expr_ordem(colunas):
    if "nrlan" in colunas:
        return "TRY_CAST(nrlan AS BIGINT)"
    return "TRY_CAST(numdoc AS BIGINT)"


def _carregar_saldoit(engine, codigo_item=None):
    sql = "SELECT cditem, cdemp, saldo FROM dbo.t_saldoit"
    params = None
    if codigo_item is not None:
        sql += " WHERE cditem = :codigo_item"
        params = {"codigo_item": codigo_item}
    return pd.read_sql(text(sql), engine, params=params)


def _carregar_movest(engine, data_corte, codigo_item=None):
    colunas = _colunas_tabela(engine, "T_MOVEST")
    coluna_data = _resolver_coluna_data(colunas)
    expr_seqit = _resolver_expr_seqit(colunas)
    expr_ordem = _resolver_expr_ordem(colunas)

    sql = f"""
        SELECT
            {coluna_data} AS data_mov,
            cditem,
            cdemp,
            qtde,
            st,
            saldoant,
            SldAntEmp,
            numdoc,
            {expr_seqit} AS seqit_sort,
            {expr_ordem} AS ordem_sort
        FROM dbo.T_MOVEST
        WHERE {coluna_data} >= :data_corte
    """

    params = {"data_corte": data_corte}
    if codigo_item is not None:
        sql += " AND cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return df

    df["qtde"] = pd.to_numeric(df["qtde"], errors="coerce").fillna(0)
    df["saldoant"] = pd.to_numeric(df["saldoant"], errors="coerce").fillna(0)
    df["SldAntEmp"] = pd.to_numeric(df["SldAntEmp"], errors="coerce").fillna(0)
    df["ordem_sort"] = pd.to_numeric(df["ordem_sort"], errors="coerce").fillna(0)
    df["seqit_sort"] = pd.to_numeric(df["seqit_sort"], errors="coerce").fillna(0)
    df["numdoc_sort"] = pd.to_numeric(df["numdoc"], errors="coerce").fillna(0)
    return df.sort_values(
        by=["cditem", "data_mov", "ordem_sort", "numdoc_sort", "seqit_sort", "cdemp"]
    ).reset_index(drop=True)


def _delta_movimento(qtde, st):
    st_upper = str(st).upper()
    if st_upper == "E":
        return qtde
    if st_upper == "S":
        return -qtde
    return 0


def _add_discrepancia(discrepancias, tipo, cditem, esperado, encontrado, **extras):
    discrepancias.append(
        {
            "tipo": tipo,
            "cditem": cditem,
            "cdemp": extras.get("cdemp"),
            "data_mov": extras.get("data_mov"),
            "numdoc": extras.get("numdoc"),
            "seqit": extras.get("seqit"),
            "st": extras.get("st"),
            "qtde": extras.get("qtde"),
            "esperado": esperado,
            "encontrado": encontrado,
            "diferenca": encontrado - esperado,
            "detalhe": extras.get("detalhe"),
        }
    )


def auditar_movest(engine_base, engine_atual, data_corte, codigo_item=None):
    df_saldo_base = _carregar_saldoit(engine_base, codigo_item=codigo_item)
    df_saldo_atual = _carregar_saldoit(engine_atual, codigo_item=codigo_item)
    df_mov = _carregar_movest(engine_atual, data_corte, codigo_item=codigo_item)

    saldo_base_item = df_saldo_base.groupby("cditem")["saldo"].sum().to_dict()
    saldo_base_emp = df_saldo_base.set_index(["cditem", "cdemp"])["saldo"].to_dict()
    saldo_atual_item = df_saldo_atual.groupby("cditem")["saldo"].sum().to_dict()
    saldo_atual_emp = df_saldo_atual.set_index(["cditem", "cdemp"])["saldo"].to_dict()

    saldo_calc_item = dict(saldo_base_item)
    saldo_calc_emp = dict(saldo_base_emp)
    empresas_por_item = {}
    discrepancias = []

    if not df_mov.empty:
        for row in df_mov.to_dict("records"):
            cditem = row["cditem"]
            cdemp = row["cdemp"]
            empresas_por_item.setdefault(cditem, set()).add(cdemp)

            esperado_geral = float(saldo_calc_item.get(cditem, 0))
            encontrado_geral = float(row["saldoant"])
            if abs(encontrado_geral - esperado_geral) > 0.000001:
                _add_discrepancia(
                    discrepancias,
                    "saldoant_movest",
                    cditem,
                    esperado_geral,
                    encontrado_geral,
                    cdemp=cdemp,
                    data_mov=row["data_mov"],
                    numdoc=row["numdoc"],
                    seqit=row["seqit_sort"],
                    st=row["st"],
                    qtde=row["qtde"],
                    detalhe="Saldo geral anterior divergente do acumulado anterior do item.",
                )

            esperado_emp = float(saldo_calc_emp.get((cditem, cdemp), 0))
            encontrado_emp = float(row["SldAntEmp"])
            if abs(encontrado_emp - esperado_emp) > 0.000001:
                _add_discrepancia(
                    discrepancias,
                    "sldantemp_movest",
                    cditem,
                    esperado_emp,
                    encontrado_emp,
                    cdemp=cdemp,
                    data_mov=row["data_mov"],
                    numdoc=row["numdoc"],
                    seqit=row["seqit_sort"],
                    st=row["st"],
                    qtde=row["qtde"],
                    detalhe="Saldo por empresa anterior divergente do acumulado anterior do item/empresa.",
                )

            delta = _delta_movimento(float(row["qtde"]), row["st"])
            saldo_calc_item[cditem] = esperado_geral + delta
            saldo_calc_emp[(cditem, cdemp)] = esperado_emp + delta

    itens_auditados = set(saldo_base_item) | set(saldo_atual_item) | set(empresas_por_item)
    if codigo_item is not None:
        itens_auditados.add(codigo_item)

    for cditem in sorted(itens_auditados):
        esperado_geral = float(saldo_calc_item.get(cditem, saldo_base_item.get(cditem, 0)))
        encontrado_geral = float(saldo_atual_item.get(cditem, 0))
        if abs(encontrado_geral - esperado_geral) > 0.000001:
            _add_discrepancia(
                discrepancias,
                "saldo_final_geral_t_saldoit",
                cditem,
                esperado_geral,
                encontrado_geral,
                detalhe="Soma final do item em t_saldoit divergente do saldo calculado pela T_MOVEST.",
            )

        empresas_item = {
            emp for item, emp in saldo_base_emp if item == cditem
        } | {
            emp for item, emp in saldo_atual_emp if item == cditem
        } | empresas_por_item.get(cditem, set())

        for cdemp in sorted(empresas_item):
            esperado_emp = float(saldo_calc_emp.get((cditem, cdemp), saldo_base_emp.get((cditem, cdemp), 0)))
            encontrado_emp = float(saldo_atual_emp.get((cditem, cdemp), 0))
            if abs(encontrado_emp - esperado_emp) > 0.000001:
                _add_discrepancia(
                    discrepancias,
                    "saldo_final_empresa_t_saldoit",
                    cditem,
                    esperado_emp,
                    encontrado_emp,
                    cdemp=cdemp,
                    detalhe="Saldo final do item/empresa em t_saldoit divergente do saldo calculado pela T_MOVEST.",
                )

    df_discrepancias = pd.DataFrame(discrepancias)
    resumo = {
        "data_corte": data_corte,
        "codigo_item": codigo_item,
        "qtd_movimentos_auditados": int(len(df_mov)),
        "qtd_itens_auditados": int(len(itens_auditados)),
        "qtd_discrepancias": int(len(df_discrepancias)),
    }
    return df_discrepancias, resumo


def salvar_relatorio_auditoria(df_discrepancias, codigo_item=None):
    pasta_saida = Path("relatorios_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo_item = f"_item_{codigo_item}" if codigo_item is not None else "_todos"
    caminho_saida = pasta_saida / f"auditoria_movest{sufixo_item}_{timestamp}.csv"

    if df_discrepancias.empty:
        pd.DataFrame(
            columns=[
                "tipo",
                "cditem",
                "cdemp",
                "data_mov",
                "numdoc",
                "seqit",
                "st",
                "qtde",
                "esperado",
                "encontrado",
                "diferenca",
                "detalhe",
            ]
        ).to_csv(caminho_saida, index=False, encoding="utf-8-sig")
    else:
        df_discrepancias.to_csv(caminho_saida, index=False, encoding="utf-8-sig")

    return caminho_saida
