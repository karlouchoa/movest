from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text

from src.transform import extrair_movimentacoes_novas

TOLERANCIA_AUDITORIA = 0.000001
CHAVE_AUDITORIA_COLUNAS = [
    "_data_key",
    "_numdoc_key",
    "_cdemp_key",
    "_cditem_key",
    "_qtde_key",
    "_especie_key",
    "_st_key",
]
CHAVE_DUPLICIDADE_VENDA_COLUNAS = [
    "_numdoc_venda_key",
    "_cdemp_venda_key",
    "_cditem_venda_key",
    "_qtde_venda_key",
]
CHAVE_DUPLICIDADE_ORIGEM_COLUNAS = [
    "_numdoc_origem_key",
    "_cdemp_origem_key",
    "_cditem_origem_key",
    "_qtde_origem_key",
    "_especie_origem_key",
    "_st_origem_key",
]


def _partes_tabela(tabela):
    valor = str(tabela).strip()
    if not valor:
        raise ValueError("O nome da tabela nao pode ficar vazio.")
    if "." in valor:
        schema, nome = valor.split(".", 1)
        return schema.strip("[]"), nome.strip("[]")
    return "dbo", valor.strip("[]")


def _q(value):
    return "[" + str(value).replace("]", "]]") + "]"


def _nome_tabela_sql(tabela):
    schema, nome = _partes_tabela(tabela)
    return f"{_q(schema)}.{_q(nome)}"


def _colunas_tabela(engine, tabela):
    schema, nome = _partes_tabela(tabela)
    inspector = inspect(engine)
    return {c["name"].lower() for c in inspector.get_columns(nome, schema=schema)}


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


def _resolver_expr_nrlan(colunas):
    if "nrlan" in colunas:
        return "TRY_CAST(nrlan AS BIGINT)"
    return "CAST(NULL AS BIGINT)"


def _carregar_saldoit(engine, codigo_item=None, codigo_empresa=None, tabela="t_saldoit"):
    colunas = _colunas_tabela(engine, tabela)
    expr_empitem = "empitem" if "empitem" in colunas else "CAST(1 AS INT) AS empitem"
    sql = f"SELECT cditem, cdemp, {expr_empitem}, saldo FROM {_nome_tabela_sql(tabela)}"
    filtros = []
    params = {}
    if codigo_item is not None:
        filtros.append("cditem = :codigo_item")
        params["codigo_item"] = codigo_item
    if codigo_empresa is not None:
        filtros.append("cdemp = :codigo_empresa")
        params["codigo_empresa"] = codigo_empresa
    if filtros:
        sql += " WHERE " + " AND ".join(filtros)
    return pd.read_sql(text(sql), engine, params=params)


def _carregar_ultimo_saldo_movest(engine, codigo_item=None, codigo_empresa=None):
    colunas = _colunas_tabela(engine, "T_MOVEST")
    coluna_nrlan = _resolver_expr_nrlan(colunas)
    coluna_data = _resolver_coluna_data(colunas)
    coluna_seqit = _resolver_expr_seqit(colunas)
    coluna_numdoc = "TRY_CAST(numdoc AS BIGINT)" if "numdoc" in colunas else "CAST(NULL AS BIGINT)"
    expr_empitem = "empitem" if "empitem" in colunas else "CAST(1 AS INT)"

    filtros = []
    params = {}
    if codigo_item is not None:
        filtros.append("m.cditem = :codigo_item")
        params["codigo_item"] = codigo_item
    if codigo_empresa is not None:
        filtros.append("m.cdemp = :codigo_empresa")
        params["codigo_empresa"] = codigo_empresa

    where_sql = ""
    if filtros:
        where_sql = "WHERE " + " AND ".join(filtros)

    sql = f"""
        WITH ultimos AS (
            SELECT
                m.cditem,
                m.cdemp,
                {expr_empitem} AS empitem,
                {coluna_data} AS data_mov,
                m.numdoc,
                {coluna_seqit} AS seqit_sort,
                m.qtde,
                m.st,
                ISNULL(m.SldAntEmp, 0) AS sldantemp_ultimo,
                CASE
                    WHEN m.st = 'E' THEN ISNULL(m.SldAntEmp, 0) + ISNULL(m.qtde, 0)
                    WHEN m.st = 'S' THEN ISNULL(m.SldAntEmp, 0) - ISNULL(m.qtde, 0)
                    ELSE ISNULL(m.SldAntEmp, 0)
                END AS saldo_final,
                {coluna_nrlan} AS nrlan_sort,
                ROW_NUMBER() OVER (
                    PARTITION BY m.cditem, m.cdemp, {expr_empitem}
                    ORDER BY {coluna_nrlan} DESC, {coluna_data} DESC, {coluna_seqit} DESC, {coluna_numdoc} DESC
                ) AS rn
            FROM dbo.T_MOVEST m
            {where_sql}
        )
        SELECT cditem, cdemp, empitem, data_mov, numdoc, seqit_sort, qtde, st, sldantemp_ultimo, saldo_final, nrlan_sort
        FROM ultimos
        WHERE rn = 1
    """
    return pd.read_sql(text(sql), engine, params=params)


def _carregar_movest(engine, data_corte, codigo_item=None, codigo_empresa=None):
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
        WHERE {coluna_data} > :data_corte
    """

    params = {"data_corte": data_corte}
    if codigo_item is not None:
        sql += " AND cditem = :codigo_item"
        params["codigo_item"] = codigo_item
    if codigo_empresa is not None:
        sql += " AND cdemp = :codigo_empresa"
        params["codigo_empresa"] = codigo_empresa

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
    registro = {
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
    for chave, valor in extras.items():
        if chave not in registro:
            registro[chave] = valor
    discrepancias.append(registro)


def _carregar_movest_detalhado(
    engine,
    data_corte,
    codigo_item=None,
    tabela="T_MOVEST",
    incluir_data_corte=True,
):
    colunas = _colunas_tabela(engine, tabela)
    coluna_data = _resolver_coluna_data(colunas)
    expr_seqit = _resolver_expr_seqit(colunas)
    expr_ordem = _resolver_expr_ordem(colunas)
    expr_nrlan = _resolver_expr_nrlan(colunas)
    nome_tabela = _nome_tabela_sql(tabela)
    operador_data = ">=" if incluir_data_corte else ">"

    sql = f"""
        SELECT
            {coluna_data} AS data_mov,
            cditem,
            cdemp,
            qtde,
            especie,
            st,
            saldoant,
            SldAntEmp,
            numdoc,
            {expr_seqit} AS seqit_atual,
            {expr_ordem} AS ordem_sort,
            {expr_nrlan} AS nrlan_atual
        FROM {nome_tabela}
        WHERE {coluna_data} {operador_data} :data_corte
    """

    params = {"data_corte": data_corte}
    if codigo_item is not None:
        sql += " AND cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "data_mov",
                "cditem",
                "cdemp",
                "qtde",
                "especie",
                "st",
                "saldoant_atual",
                "sldantemp_atual",
                "numdoc",
                "seqit_atual",
                "ordem_sort",
                "nrlan_atual",
                "numdoc_sort",
            ]
        )

    df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")
    df["qtde"] = pd.to_numeric(df["qtde"], errors="coerce").fillna(0)
    df["saldoant"] = pd.to_numeric(df["saldoant"], errors="coerce").fillna(0)
    df["SldAntEmp"] = pd.to_numeric(df["SldAntEmp"], errors="coerce").fillna(0)
    df["ordem_sort"] = pd.to_numeric(df["ordem_sort"], errors="coerce").fillna(0)
    df["seqit_atual"] = pd.to_numeric(df["seqit_atual"], errors="coerce")
    df["nrlan_atual"] = pd.to_numeric(df["nrlan_atual"], errors="coerce")
    df["numdoc_sort"] = pd.to_numeric(df["numdoc"], errors="coerce").fillna(0)
    df["especie"] = df["especie"].astype("string").fillna("").str.strip().str.upper()
    df["st"] = df["st"].astype("string").fillna("").str.strip().str.upper()
    df = df.rename(columns={"saldoant": "saldoant_atual", "SldAntEmp": "sldantemp_atual"})
    return _ordenar_movimentos(df, "nrlan_atual")


def _carregar_ajustes_inventario_posteriores(engine, data_corte, codigo_item=None):
    df = _carregar_movest_detalhado(
        engine,
        data_corte,
        codigo_item=codigo_item,
        incluir_data_corte=False,
    )
    if df.empty:
        return df

    df = df[df["especie"] == "I"].copy()
    if df.empty:
        return df

    return df.reset_index(drop=True)


def _ordenar_movimentos(df, coluna_desempate=None):
    if df.empty:
        return df.reset_index(drop=True)

    coluna_seqit = None
    for candidata in ("seqit_sort", "seqit_atual", "seqit_esperado"):
        if candidata in df.columns:
            coluna_seqit = candidata
            break

    colunas_ordenacao = [
        "cditem",
        "data_mov",
        "ordem_sort",
        "numdoc_sort",
        coluna_seqit,
        "cdemp",
        coluna_desempate,
    ]
    colunas_ordenacao = [col for col in colunas_ordenacao if col and col in df.columns]
    return df.sort_values(by=colunas_ordenacao).reset_index(drop=True)


def _simular_movimentos_esperados(
    engine_base,
    engine_atual,
    data_corte,
    codigo_item=None,
    tabela_inventario=None,
    importar_ajuste_inventario=True,
):
    df_esperado = extrair_movimentacoes_novas(
        engine_atual,
        data_corte,
        tabela_inventario=tabela_inventario,
        codigo_item=codigo_item,
        importar_ajuste_inventario=importar_ajuste_inventario,
    ).copy()

    if df_esperado.empty:
        return pd.DataFrame(
            columns=[
                "data_mov",
                "cditem",
                "cdemp",
                "qtde",
                "especie",
                "st",
                "saldoant_esperado",
                "sldantemp_esperado",
                "numdoc",
                "seqit_esperado",
                "ordem_sort",
                "nrlan_esperado",
                "numdoc_sort",
            ]
        )

    df_esperado["data_mov"] = pd.to_datetime(
        df_esperado["DataLan"] if "DataLan" in df_esperado.columns else df_esperado["data"],
        errors="coerce",
    )
    df_esperado = df_esperado[df_esperado["data_mov"] > pd.to_datetime(data_corte)].copy()
    if df_esperado.empty:
        return pd.DataFrame(
            columns=[
                "data_mov",
                "cditem",
                "cdemp",
                "qtde",
                "especie",
                "st",
                "saldoant_esperado",
                "sldantemp_esperado",
                "numdoc",
                "seqit_esperado",
                "ordem_sort",
                "nrlan_esperado",
                "numdoc_sort",
            ]
        )
    df_esperado["qtde"] = pd.to_numeric(df_esperado["qtde"], errors="coerce").fillna(0)
    df_esperado["ordem_sort"] = pd.to_numeric(df_esperado["_ordem"], errors="coerce").fillna(0)
    df_esperado["seqit_esperado"] = pd.to_numeric(df_esperado["SEQIT"], errors="coerce")
    df_esperado["nrlan_esperado"] = pd.to_numeric(
        df_esperado["_nrlan_origem"], errors="coerce"
    )
    df_esperado["numdoc_sort"] = pd.to_numeric(df_esperado["numdoc"], errors="coerce").fillna(0)
    df_esperado["especie"] = df_esperado["especie"].astype("string").fillna("").str.strip().str.upper()
    df_esperado["st"] = df_esperado["st"].astype("string").fillna("").str.strip().str.upper()

    df_esperado = _ordenar_movimentos(df_esperado, "nrlan_esperado")

    df_saldo_base = _carregar_saldoit(engine_base, codigo_item=codigo_item)
    saldo_item = df_saldo_base.groupby("cditem")["saldo"].sum().to_dict()
    saldo_emp = df_saldo_base.set_index(["cditem", "cdemp"])["saldo"].to_dict()

    saldos_anteriores = []
    saldos_anteriores_emp = []

    for row in df_esperado.to_dict("records"):
        cditem = row["cditem"]
        cdemp = row["cdemp"]
        saldo_geral_atual = float(saldo_item.get(cditem, 0))
        saldo_empresa_atual = float(saldo_emp.get((cditem, cdemp), 0))

        saldos_anteriores.append(saldo_geral_atual)
        saldos_anteriores_emp.append(saldo_empresa_atual)

        delta = _delta_movimento(float(row["qtde"]), row["st"])
        saldo_item[cditem] = saldo_geral_atual + delta
        saldo_emp[(cditem, cdemp)] = saldo_empresa_atual + delta

    df_esperado["saldoant_esperado"] = saldos_anteriores
    df_esperado["sldantemp_esperado"] = saldos_anteriores_emp

    return df_esperado[
        [
            "data_mov",
            "cditem",
            "cdemp",
            "qtde",
            "especie",
            "st",
            "saldoant_esperado",
            "sldantemp_esperado",
            "numdoc",
            "seqit_esperado",
            "ordem_sort",
            "nrlan_esperado",
            "numdoc_sort",
        ]
    ].copy()


def _preparar_chaves_auditoria(df):
    if df.empty:
        for coluna in CHAVE_AUDITORIA_COLUNAS + ["ordem_no_grupo", "chave_negocio"]:
            df[coluna] = pd.Series(dtype="object")
        return df

    df = df.copy()
    df["_data_key"] = pd.to_datetime(df["data_mov"], errors="coerce")
    df["_numdoc_key"] = df["numdoc"].astype("string").fillna("").str.strip()
    df["_cdemp_key"] = pd.to_numeric(df["cdemp"], errors="coerce")
    df["_cditem_key"] = pd.to_numeric(df["cditem"], errors="coerce")
    df["_qtde_key"] = pd.to_numeric(df["qtde"], errors="coerce").fillna(0).round(4)
    df["_especie_key"] = df["especie"].astype("string").fillna("").str.strip().str.upper()
    df["_st_key"] = df["st"].astype("string").fillna("").str.strip().str.upper()
    df["ordem_no_grupo"] = df.groupby(CHAVE_AUDITORIA_COLUNAS, dropna=False).cumcount() + 1
    df["chave_negocio"] = df.apply(_montar_chave_negocio, axis=1)
    return df


def _montar_chave_negocio(row):
    data_mov = pd.to_datetime(row.get("data_mov"), errors="coerce")
    if pd.isna(data_mov):
        data_texto = ""
    else:
        data_texto = data_mov.strftime("%Y-%m-%d %H:%M:%S")

    return (
        f"DataLan={data_texto}; numdoc={row.get('numdoc')}; cdemp={row.get('cdemp')}; "
        f"cditem={row.get('cditem')}; qtde={row.get('qtde')}; "
        f"especie={row.get('especie')}; st={row.get('st')}"
    )


def _serie_para_lista_texto(serie):
    valores = []
    for valor in serie.dropna().tolist():
        if isinstance(valor, float) and valor.is_integer():
            valores.append(str(int(valor)))
        else:
            valores.append(str(valor))
    return ", ".join(valores)


def _contagens_por_chave(df, nome_coluna):
    if df.empty:
        return pd.DataFrame(columns=CHAVE_AUDITORIA_COLUNAS + [nome_coluna])
    return (
        df.groupby(CHAVE_AUDITORIA_COLUNAS, dropna=False)
        .size()
        .reset_index(name=nome_coluna)
    )


def _carregar_contagem_vendas_origem(engine, data_corte, codigo_item=None):
    sql = """
        SELECT
            v.nrven_v AS numdoc,
            iv.cdemp_iv AS cdemp,
            iv.cditem_iv AS cditem,
            CAST(ISNULL(iv.qtdeSol_iv, 0) AS DECIMAL(18, 4)) AS qtde,
            COUNT(*) AS qtd_esperada_venda
        FROM T_ITSVEN iv
        INNER JOIN T_VENDAS v
            ON v.nrven_v = iv.nrven_iv
        WHERE v.emisven_v > :data_corte
          AND ISNULL(v.TrocReq, 'N') <> 'S'
    """
    params = {"data_corte": data_corte}
    if codigo_item is not None:
        sql += " AND iv.cditem_iv = :codigo_item"
        params["codigo_item"] = codigo_item
    sql += """
        GROUP BY
            v.nrven_v,
            iv.cdemp_iv,
            iv.cditem_iv,
            CAST(ISNULL(iv.qtdeSol_iv, 0) AS DECIMAL(18, 4))
    """

    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return pd.DataFrame(
            columns=["numdoc", "cdemp", "cditem", "qtde", "qtd_esperada_venda"]
        )

    df["numdoc"] = df["numdoc"].astype("string").fillna("").str.strip()
    df["cdemp"] = pd.to_numeric(df["cdemp"], errors="coerce")
    df["cditem"] = pd.to_numeric(df["cditem"], errors="coerce")
    df["qtde"] = pd.to_numeric(df["qtde"], errors="coerce").fillna(0).round(4)
    df["qtd_esperada_venda"] = (
        pd.to_numeric(df["qtd_esperada_venda"], errors="coerce").fillna(0).astype(int)
    )
    return df


def _carregar_contagem_pdc_origem(engine, data_corte, codigo_item=None):
    sql = """
        SELECT
            p.nrNFC AS numdoc,
            p.empent AS cdemp,
            it.cditem AS cditem,
            CAST(ISNULL(it.QtSol, 0) AS DECIMAL(18, 4)) AS qtde,
            CASE WHEN p.StaReq = 'E' THEN 'C' ELSE 'D' END AS especie,
            'E' AS st,
            COUNT(*) AS qtd_esperada_origem
        FROM T_ITPDC it
        INNER JOIN T_PDC p
            ON p.NrReq = it.Nrreq
        WHERE p.DtSta > :data_corte
          AND p.StaReq IN ('E', 'A')
    """
    params = {"data_corte": data_corte}
    if codigo_item is not None:
        sql += " AND it.cditem = :codigo_item"
        params["codigo_item"] = codigo_item
    sql += """
        GROUP BY
            p.nrNFC,
            p.empent,
            it.cditem,
            CAST(ISNULL(it.QtSol, 0) AS DECIMAL(18, 4)),
            CASE WHEN p.StaReq = 'E' THEN 'C' ELSE 'D' END
    """
    return _normalizar_contagem_origem(pd.read_sql(text(sql), engine, params=params))


def _carregar_contagem_transferencias_origem(engine, data_corte, codigo_item=None):
    filtro_item = ""
    params = {"data_corte": data_corte}
    if codigo_item is not None:
        filtro_item = " AND it.cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    sql = f"""
        SELECT
            t.codtransf AS numdoc,
            t.cdempsaida AS cdemp,
            it.cditem AS cditem,
            CAST(ISNULL(it.qtditem, 0) AS DECIMAL(18, 4)) AS qtde,
            'F' AS especie,
            'S' AS st,
            COUNT(*) AS qtd_esperada_origem
        FROM T_ITTRANSF it
        INNER JOIN T_TRANSF t
            ON t.codtransf = it.cdtransf
        WHERE t.datahorarec > :data_corte
          AND t.statustransf = 'E'
          {filtro_item}
        GROUP BY
            t.codtransf,
            t.cdempsaida,
            it.cditem,
            CAST(ISNULL(it.qtditem, 0) AS DECIMAL(18, 4))

        UNION ALL

        SELECT
            t.codtransf AS numdoc,
            t.cdempentrada AS cdemp,
            it.cditem AS cditem,
            CAST(ISNULL(it.qtditem, 0) AS DECIMAL(18, 4)) AS qtde,
            'F' AS especie,
            'E' AS st,
            COUNT(*) AS qtd_esperada_origem
        FROM T_ITTRANSF it
        INNER JOIN T_TRANSF t
            ON t.codtransf = it.cdtransf
        WHERE t.datahorarec > :data_corte
          AND t.statustransf = 'E'
          {filtro_item}
        GROUP BY
            t.codtransf,
            t.cdempentrada,
            it.cditem,
            CAST(ISNULL(it.qtditem, 0) AS DECIMAL(18, 4))
    """
    return _normalizar_contagem_origem(pd.read_sql(text(sql), engine, params=params))


def _normalizar_contagem_origem(df):
    if df.empty:
        return pd.DataFrame(
            columns=["numdoc", "cdemp", "cditem", "qtde", "especie", "st", "qtd_esperada_origem"]
        )
    df = df.copy()
    df["numdoc"] = df["numdoc"].astype("string").fillna("").str.strip()
    df["cdemp"] = pd.to_numeric(df["cdemp"], errors="coerce")
    df["cditem"] = pd.to_numeric(df["cditem"], errors="coerce")
    df["qtde"] = pd.to_numeric(df["qtde"], errors="coerce").fillna(0).round(4)
    df["especie"] = df["especie"].astype("string").fillna("").str.strip().str.upper()
    df["st"] = df["st"].astype("string").fillna("").str.strip().str.upper()
    df["qtd_esperada_origem"] = (
        pd.to_numeric(df["qtd_esperada_origem"], errors="coerce").fillna(0).astype(int)
    )
    return df


def _identificar_duplicidades_venda(df_atual, df_vendas_origem):
    colunas_saida = list(df_atual.columns) + ["qtd_encontrada_grupo", "qtd_esperada_grupo"]
    if df_atual.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_vendas = df_atual[
        (df_atual["especie"] == "V") & (df_atual["st"] == "S")
    ].copy()
    if df_vendas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_vendas["_numdoc_venda_key"] = df_vendas["numdoc"].astype("string").fillna("").str.strip()
    df_vendas["_cdemp_venda_key"] = pd.to_numeric(df_vendas["cdemp"], errors="coerce")
    df_vendas["_cditem_venda_key"] = pd.to_numeric(df_vendas["cditem"], errors="coerce")
    df_vendas["_qtde_venda_key"] = pd.to_numeric(df_vendas["qtde"], errors="coerce").fillna(0).round(4)

    if df_vendas_origem.empty:
        df_origem = pd.DataFrame(columns=CHAVE_DUPLICIDADE_VENDA_COLUNAS + ["qtd_esperada_venda"])
    else:
        df_origem = df_vendas_origem.copy()
        df_origem = df_origem.rename(
            columns={
                "numdoc": "_numdoc_venda_key",
                "cdemp": "_cdemp_venda_key",
                "cditem": "_cditem_venda_key",
                "qtde": "_qtde_venda_key",
            }
        )

    contagens_atuais = (
        df_vendas.groupby(CHAVE_DUPLICIDADE_VENDA_COLUNAS, dropna=False)
        .size()
        .reset_index(name="qtd_encontrada_grupo")
    )
    contagens = contagens_atuais.merge(
        df_origem[CHAVE_DUPLICIDADE_VENDA_COLUNAS + ["qtd_esperada_venda"]],
        on=CHAVE_DUPLICIDADE_VENDA_COLUNAS,
        how="left",
    )
    contagens["qtd_esperada_venda"] = (
        pd.to_numeric(contagens["qtd_esperada_venda"], errors="coerce").fillna(0).astype(int)
    )
    contagens = contagens[contagens["qtd_encontrada_grupo"] > contagens["qtd_esperada_venda"]].copy()
    if contagens.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_vendas = df_vendas.merge(
        contagens,
        on=CHAVE_DUPLICIDADE_VENDA_COLUNAS,
        how="inner",
    )
    df_vendas["ordem_duplicidade_venda"] = (
        df_vendas.groupby(CHAVE_DUPLICIDADE_VENDA_COLUNAS, dropna=False).cumcount() + 1
    )
    df_vendas = df_vendas[
        df_vendas["ordem_duplicidade_venda"] > df_vendas["qtd_esperada_venda"]
    ].copy()
    if df_vendas.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_vendas = df_vendas.rename(columns={"qtd_esperada_venda": "qtd_esperada_grupo"})
    return df_vendas[colunas_saida].reset_index(drop=True)


def _identificar_duplicidades_origem(df_atual, df_origem):
    colunas_saida = list(df_atual.columns) + ["qtd_encontrada_grupo", "qtd_esperada_grupo"]
    if df_atual.empty or df_origem.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_base = df_atual.copy()
    df_base["_numdoc_origem_key"] = df_base["numdoc"].astype("string").fillna("").str.strip()
    df_base["_cdemp_origem_key"] = pd.to_numeric(df_base["cdemp"], errors="coerce")
    df_base["_cditem_origem_key"] = pd.to_numeric(df_base["cditem"], errors="coerce")
    df_base["_qtde_origem_key"] = pd.to_numeric(df_base["qtde"], errors="coerce").fillna(0).round(4)
    df_base["_especie_origem_key"] = (
        df_base["especie"].astype("string").fillna("").str.strip().str.upper()
    )
    df_base["_st_origem_key"] = df_base["st"].astype("string").fillna("").str.strip().str.upper()

    df_origem_norm = df_origem.rename(
        columns={
            "numdoc": "_numdoc_origem_key",
            "cdemp": "_cdemp_origem_key",
            "cditem": "_cditem_origem_key",
            "qtde": "_qtde_origem_key",
            "especie": "_especie_origem_key",
            "st": "_st_origem_key",
        }
    ).copy()

    df_base = df_base.merge(
        df_origem_norm[CHAVE_DUPLICIDADE_ORIGEM_COLUNAS].drop_duplicates(),
        on=CHAVE_DUPLICIDADE_ORIGEM_COLUNAS,
        how="inner",
    )
    if df_base.empty:
        return pd.DataFrame(columns=colunas_saida)

    contagens = (
        df_base.groupby(CHAVE_DUPLICIDADE_ORIGEM_COLUNAS, dropna=False)
        .size()
        .reset_index(name="qtd_encontrada_grupo")
    )
    contagens = contagens.merge(
        df_origem_norm[CHAVE_DUPLICIDADE_ORIGEM_COLUNAS + ["qtd_esperada_origem"]],
        on=CHAVE_DUPLICIDADE_ORIGEM_COLUNAS,
        how="left",
    )
    contagens["qtd_esperada_origem"] = (
        pd.to_numeric(contagens["qtd_esperada_origem"], errors="coerce").fillna(0).astype(int)
    )
    contagens = contagens[
        contagens["qtd_encontrada_grupo"] > contagens["qtd_esperada_origem"]
    ].copy()
    if contagens.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_base = df_base.merge(
        contagens,
        on=CHAVE_DUPLICIDADE_ORIGEM_COLUNAS,
        how="inner",
    )
    df_base["ordem_duplicidade_origem"] = (
        df_base.groupby(CHAVE_DUPLICIDADE_ORIGEM_COLUNAS, dropna=False).cumcount() + 1
    )
    df_base = df_base[
        df_base["ordem_duplicidade_origem"] > df_base["qtd_esperada_origem"]
    ].copy()
    if df_base.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_base = df_base.rename(columns={"qtd_esperada_origem": "qtd_esperada_grupo"})
    return df_base[colunas_saida].reset_index(drop=True)


def _identificar_duplicidades_pdc(df_atual, df_pdc_origem):
    colunas_saida = list(df_atual.columns) + ["qtd_encontrada_grupo", "qtd_esperada_grupo"]
    if df_atual.empty or df_pdc_origem.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_base = df_atual.copy()
    df_base["_numdoc_origem_key"] = df_base["numdoc"].astype("string").fillna("").str.strip()
    df_base["_cdemp_origem_key"] = pd.to_numeric(df_base["cdemp"], errors="coerce")
    df_base["_cditem_origem_key"] = pd.to_numeric(df_base["cditem"], errors="coerce")
    df_base["_qtde_origem_key"] = pd.to_numeric(df_base["qtde"], errors="coerce").fillna(0).round(4)
    df_base["_especie_origem_key"] = (
        df_base["especie"].astype("string").fillna("").str.strip().str.upper()
    )
    df_base["_st_origem_key"] = df_base["st"].astype("string").fillna("").str.strip().str.upper()

    df_pdc = df_pdc_origem.rename(
        columns={
            "numdoc": "_numdoc_origem_key",
            "cdemp": "_cdemp_origem_key",
            "cditem": "_cditem_origem_key",
            "qtde": "_qtde_origem_key",
            "especie": "_especie_origem_key",
            "st": "_st_origem_key",
        }
    ).copy()

    chaves_pdc = [
        "_numdoc_origem_key",
        "_cdemp_origem_key",
        "_cditem_origem_key",
        "_qtde_origem_key",
        "_especie_origem_key",
    ]

    df_candidatos = df_base[df_base["_especie_origem_key"].isin(["C", "D"])].copy()
    if df_candidatos.empty:
        return pd.DataFrame(columns=colunas_saida)

    contagens_mov = (
        df_candidatos.groupby(chaves_pdc + ["_st_origem_key"], dropna=False)
        .size()
        .reset_index(name="qtd_mov")
    )
    contagens_e = contagens_mov[contagens_mov["_st_origem_key"] == "E"].copy()
    contagens_s = contagens_mov[contagens_mov["_st_origem_key"] == "S"].copy()
    contagens_e = contagens_e.rename(columns={"qtd_mov": "qtd_e"})
    contagens_s = contagens_s.rename(columns={"qtd_mov": "qtd_s"})

    contagens = contagens_e[chaves_pdc + ["qtd_e"]].merge(
        contagens_s[chaves_pdc + ["qtd_s"]],
        on=chaves_pdc,
        how="left",
    )
    contagens["qtd_s"] = pd.to_numeric(contagens["qtd_s"], errors="coerce").fillna(0).astype(int)

    df_pdc_esperado = df_pdc[df_pdc["_st_origem_key"] == "E"][chaves_pdc + ["qtd_esperada_origem"]].copy()
    contagens = contagens.merge(
        df_pdc_esperado,
        on=chaves_pdc,
        how="left",
    )
    contagens["qtd_esperada_origem"] = (
        pd.to_numeric(contagens["qtd_esperada_origem"], errors="coerce").fillna(0).astype(int)
    )
    contagens["qtd_excedente_liquida"] = contagens["qtd_e"] - contagens["qtd_s"] - contagens["qtd_esperada_origem"]
    contagens = contagens[contagens["qtd_excedente_liquida"] > 0].copy()
    if contagens.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_excedentes = df_candidatos[df_candidatos["_st_origem_key"] == "E"].copy()
    df_excedentes = df_excedentes.merge(
        contagens[chaves_pdc + ["qtd_e", "qtd_s", "qtd_esperada_origem", "qtd_excedente_liquida"]],
        on=chaves_pdc,
        how="inner",
    )
    df_excedentes["ordem_entrada"] = df_excedentes.groupby(chaves_pdc, dropna=False).cumcount() + 1
    df_excedentes = df_excedentes[
        df_excedentes["ordem_entrada"] > (df_excedentes["qtd_esperada_origem"] + df_excedentes["qtd_s"])
    ].copy()
    if df_excedentes.empty:
        return pd.DataFrame(columns=colunas_saida)

    df_excedentes["qtd_encontrada_grupo"] = df_excedentes["qtd_e"] - df_excedentes["qtd_s"]
    df_excedentes["qtd_esperada_grupo"] = df_excedentes["qtd_esperada_origem"]
    return df_excedentes[colunas_saida].reset_index(drop=True)


def _identificar_cancelamentos_venda_compensados(df_atual):
    if df_atual.empty:
        return set()

    df_base = df_atual[
        (df_atual["especie"] == "V") & (df_atual["st"].isin(["E", "S"]))
    ].copy()
    if df_base.empty:
        return set()

    chaves = [
        "_numdoc_venda_cancel_key",
        "_cdemp_venda_cancel_key",
        "_cditem_venda_cancel_key",
        "_qtde_venda_cancel_key",
    ]
    df_base["_numdoc_venda_cancel_key"] = df_base["numdoc"].astype("string").fillna("").str.strip()
    df_base["_cdemp_venda_cancel_key"] = pd.to_numeric(df_base["cdemp"], errors="coerce")
    df_base["_cditem_venda_cancel_key"] = pd.to_numeric(df_base["cditem"], errors="coerce")
    df_base["_qtde_venda_cancel_key"] = (
        pd.to_numeric(df_base["qtde"], errors="coerce").fillna(0).round(4)
    )

    contagens = (
        df_base.groupby(chaves + ["st"], dropna=False)
        .size()
        .reset_index(name="qtd")
    )
    contagens_e = contagens[contagens["st"] == "E"][chaves + ["qtd"]].rename(columns={"qtd": "qtd_e"})
    contagens_s = contagens[contagens["st"] == "S"][chaves + ["qtd"]].rename(columns={"qtd": "qtd_s"})
    pares = contagens_e.merge(contagens_s, on=chaves, how="inner")
    if pares.empty:
        return set()

    pares["qtd_compensada"] = pares[["qtd_e", "qtd_s"]].min(axis=1)
    pares = pares[pares["qtd_compensada"] > 0].copy()
    if pares.empty:
        return set()

    df_e = df_base[df_base["st"] == "E"].copy().merge(
        pares[chaves + ["qtd_compensada"]],
        on=chaves,
        how="inner",
    )
    if df_e.empty:
        return set()

    df_e["ordem_cancelamento"] = df_e.groupby(chaves, dropna=False).cumcount() + 1
    df_e = df_e[df_e["ordem_cancelamento"] <= df_e["qtd_compensada"]].copy()
    return set(df_e["nrlan_atual"].dropna().tolist())


def _lista_grupo_por_chave(df, coluna_valor, nome_coluna_saida):
    if df.empty or coluna_valor not in df.columns:
        return pd.DataFrame(columns=CHAVE_AUDITORIA_COLUNAS + [nome_coluna_saida])
    return (
        df.groupby(CHAVE_AUDITORIA_COLUNAS, dropna=False)[coluna_valor]
        .apply(_serie_para_lista_texto)
        .reset_index(name=nome_coluna_saida)
    )


def _valor_float(valor):
    numero = pd.to_numeric(pd.Series([valor]), errors="coerce").iloc[0]
    if pd.isna(numero):
        return None
    return float(numero)


def _primeiro_valor_preenchido(*valores):
    for valor in valores:
        if pd.isna(valor):
            continue
        return valor
    return None


def _registro_anomalia(
    tipo,
    detalhe,
    data_mov=None,
    numdoc=None,
    cdemp=None,
    cditem=None,
    qtde=None,
    especie=None,
    st=None,
    nrlan_atual=None,
    seqit_atual=None,
    saldoant_atual=None,
    sldantemp_atual=None,
    nrlan_esperado=None,
    seqit_esperado=None,
    saldoant_esperado=None,
    sldantemp_esperado=None,
    qtd_encontrada_grupo=None,
    qtd_esperada_grupo=None,
    nrlans_grupo=None,
):
    saldoant_atual_float = _valor_float(saldoant_atual)
    saldoant_esperado_float = _valor_float(saldoant_esperado)
    sldantemp_atual_float = _valor_float(sldantemp_atual)
    sldantemp_esperado_float = _valor_float(sldantemp_esperado)

    diferenca_saldoant = None
    if saldoant_atual_float is not None and saldoant_esperado_float is not None:
        diferenca_saldoant = saldoant_atual_float - saldoant_esperado_float

    diferenca_sldantemp = None
    if sldantemp_atual_float is not None and sldantemp_esperado_float is not None:
        diferenca_sldantemp = sldantemp_atual_float - sldantemp_esperado_float

    return {
        "tipo": tipo,
        "detalhe": detalhe,
        "chave_negocio": _montar_chave_negocio(
            {
                "data_mov": data_mov,
                "numdoc": numdoc,
                "cdemp": cdemp,
                "cditem": cditem,
                "qtde": qtde,
                "especie": especie,
                "st": st,
            }
        ),
        "data_mov": data_mov,
        "numdoc": numdoc,
        "cdemp": cdemp,
        "cditem": cditem,
        "qtde": qtde,
        "especie": especie,
        "st": st,
        "nrlan_atual": nrlan_atual,
        "seqit_atual": seqit_atual,
        "saldoant_atual": saldoant_atual_float,
        "sldantemp_atual": sldantemp_atual_float,
        "nrlan_esperado": nrlan_esperado,
        "seqit_esperado": seqit_esperado,
        "saldoant_esperado": saldoant_esperado_float,
        "sldantemp_esperado": sldantemp_esperado_float,
        "diferenca_saldoant": diferenca_saldoant,
        "diferenca_sldantemp": diferenca_sldantemp,
        "qtd_encontrada_grupo": qtd_encontrada_grupo,
        "qtd_esperada_grupo": qtd_esperada_grupo,
        "nrlans_grupo": nrlans_grupo,
    }


def _formatar_data_texto(valor):
    data = pd.to_datetime(valor, errors="coerce")
    if pd.isna(data):
        return ""
    return data.strftime("%Y-%m-%d %H:%M:%S")


def _eh_movimento_posterior(data_referencia, nrlan_referencia, data_candidata, nrlan_candidato):
    data_ref = pd.to_datetime(data_referencia, errors="coerce")
    data_cand = pd.to_datetime(data_candidata, errors="coerce")
    if pd.isna(data_ref) or pd.isna(data_cand):
        return False
    if data_cand > data_ref:
        return True
    if data_cand < data_ref:
        return False

    nrlan_ref = _valor_float(nrlan_referencia)
    nrlan_cand = _valor_float(nrlan_candidato)
    if nrlan_ref is None or nrlan_cand is None:
        return False
    return nrlan_cand > nrlan_ref


def _resumir_ajustes_posteriores(df_ajustes, data_anomalia, cditem, cdemp=None, nrlan_referencia=None):
    if df_ajustes.empty or cditem is None:
        return {
            "possui_ajuste_inventario_posterior": False,
            "qtd_ajustes_inventario_posteriores": 0,
            "primeiro_ajuste_inventario_posterior": None,
            "ultimo_ajuste_inventario_posterior": None,
            "ajustes_inventario_posteriores": None,
        }

    df_filtrado = df_ajustes[df_ajustes["cditem"] == cditem].copy()
    if cdemp is not None:
        df_filtrado = df_filtrado[df_filtrado["cdemp"] == cdemp].copy()
    if df_filtrado.empty:
        return {
            "possui_ajuste_inventario_posterior": False,
            "qtd_ajustes_inventario_posteriores": 0,
            "primeiro_ajuste_inventario_posterior": None,
            "ultimo_ajuste_inventario_posterior": None,
            "ajustes_inventario_posteriores": None,
        }

    mascara_posterior = df_filtrado.apply(
        lambda row: _eh_movimento_posterior(
            data_anomalia,
            nrlan_referencia,
            row.get("data_mov"),
            row.get("nrlan_atual"),
        ),
        axis=1,
    )
    df_filtrado = df_filtrado[mascara_posterior].copy()
    if df_filtrado.empty:
        return {
            "possui_ajuste_inventario_posterior": False,
            "qtd_ajustes_inventario_posteriores": 0,
            "primeiro_ajuste_inventario_posterior": None,
            "ultimo_ajuste_inventario_posterior": None,
            "ajustes_inventario_posteriores": None,
        }

    descricoes = []
    for row in df_filtrado.to_dict("records"):
        descricoes.append(
            " | ".join(
                [
                    f"data={_formatar_data_texto(row.get('data_mov'))}",
                    f"nrlan={row.get('nrlan_atual')}",
                    f"numdoc={row.get('numdoc')}",
                    f"cdemp={row.get('cdemp')}",
                    f"st={row.get('st')}",
                    f"qtde={row.get('qtde')}",
                    f"saldoant={row.get('saldoant_atual')}",
                    f"sldantemp={row.get('sldantemp_atual')}",
                ]
            )
        )

    return {
        "possui_ajuste_inventario_posterior": True,
        "qtd_ajustes_inventario_posteriores": int(len(df_filtrado)),
        "primeiro_ajuste_inventario_posterior": _formatar_data_texto(df_filtrado["data_mov"].min()),
        "ultimo_ajuste_inventario_posterior": _formatar_data_texto(df_filtrado["data_mov"].max()),
        "ajustes_inventario_posteriores": " || ".join(descricoes),
    }


def auditar_anomalias_movest(
    engine_base,
    engine_atual,
    data_corte,
    codigo_item=None,
    codigo_empresa=None,
    tabela_inventario=None,
    importar_ajuste_inventario=True,
):
    df_atual = _carregar_movest_detalhado(
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
        incluir_data_corte=False,
    )
    df_esperado = _simular_movimentos_esperados(
        engine_base,
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
        tabela_inventario=tabela_inventario,
        importar_ajuste_inventario=importar_ajuste_inventario,
    )
    df_ajustes = _carregar_ajustes_inventario_posteriores(
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
    )
    df_vendas_origem = _carregar_contagem_vendas_origem(
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
    )
    df_pdc_origem = _carregar_contagem_pdc_origem(
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
    )
    df_transf_origem = _carregar_contagem_transferencias_origem(
        engine_atual,
        data_corte,
        codigo_item=codigo_item,
    )

    df_atual = _preparar_chaves_auditoria(df_atual)
    df_esperado = _preparar_chaves_auditoria(df_esperado)

    contagens_atual = _contagens_por_chave(df_atual, "qtd_encontrada_grupo")
    contagens_esperado = _contagens_por_chave(df_esperado, "qtd_esperada_grupo")
    contagens = contagens_atual.merge(
        contagens_esperado,
        on=CHAVE_AUDITORIA_COLUNAS,
        how="outer",
    )
    contagens["qtd_encontrada_grupo"] = (
        pd.to_numeric(contagens["qtd_encontrada_grupo"], errors="coerce").fillna(0).astype(int)
    )
    contagens["qtd_esperada_grupo"] = (
        pd.to_numeric(contagens["qtd_esperada_grupo"], errors="coerce").fillna(0).astype(int)
    )

    grupos_nrlan = _lista_grupo_por_chave(df_atual, "nrlan_atual", "nrlans_grupo")
    grupos_duplicados = contagens[
        (contagens["qtd_encontrada_grupo"] > 1)
        & ~(
            (contagens["_especie_key"] == "V")
            & (contagens["_st_key"] == "S")
        )
    ].merge(
        grupos_nrlan,
        on=CHAVE_AUDITORIA_COLUNAS,
        how="left",
    )
    df_duplicidades_venda = _identificar_duplicidades_venda(df_atual, df_vendas_origem)
    if not df_duplicidades_venda.empty:
        df_duplicidades_venda = df_duplicidades_venda.merge(
            grupos_nrlan,
            on=CHAVE_AUDITORIA_COLUNAS,
            how="left",
        )
    df_duplicidades_pdc = _identificar_duplicidades_pdc(df_atual, df_pdc_origem)
    if not df_duplicidades_pdc.empty:
        df_duplicidades_pdc = df_duplicidades_pdc.merge(
            grupos_nrlan,
            on=CHAVE_AUDITORIA_COLUNAS,
            how="left",
        )
    df_duplicidades_transf = _identificar_duplicidades_origem(df_atual, df_transf_origem)
    if not df_duplicidades_transf.empty:
        df_duplicidades_transf = df_duplicidades_transf.merge(
            grupos_nrlan,
            on=CHAVE_AUDITORIA_COLUNAS,
            how="left",
        )

    chaves_merge = CHAVE_AUDITORIA_COLUNAS + ["ordem_no_grupo"]
    df_comparacao = df_atual.merge(
        df_esperado,
        on=chaves_merge,
        how="outer",
        suffixes=("_atual", "_esperado"),
        indicator=True,
    )
    df_comparacao = df_comparacao.merge(
        contagens,
        on=CHAVE_AUDITORIA_COLUNAS,
        how="left",
    )
    df_comparacao = df_comparacao.merge(
        grupos_nrlan,
        on=CHAVE_AUDITORIA_COLUNAS,
        how="left",
    )

    anomalias = []
    nrlans_duplicidade_origem = set()
    for df_excesso in (df_duplicidades_venda, df_duplicidades_pdc, df_duplicidades_transf):
        if not df_excesso.empty and "nrlan_atual" in df_excesso.columns:
            for valor in df_excesso["nrlan_atual"].dropna().tolist():
                nrlans_duplicidade_origem.add(valor)
    nrlans_cancelamentos_venda_compensados = _identificar_cancelamentos_venda_compensados(df_atual)

    if not df_duplicidades_venda.empty:
        for row in df_duplicidades_venda.to_dict("records"):
            anomalias.append(
                _registro_anomalia(
                    tipo="duplicidade_venda",
                    detalhe=(
                        "A quantidade de registros de venda na T_MOVEST excede a quantidade "
                        "de lancamentos equivalentes na T_ITSVEN para o mesmo numdoc/cdemp/cditem/qtde."
                    ),
                    data_mov=row.get("data_mov"),
                    numdoc=row.get("numdoc"),
                    cdemp=row.get("cdemp"),
                    cditem=row.get("cditem"),
                    qtde=row.get("qtde"),
                    especie=row.get("especie"),
                    st=row.get("st"),
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=row.get("sldantemp_atual"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

    if not df_duplicidades_pdc.empty:
        for row in df_duplicidades_pdc.to_dict("records"):
            anomalias.append(
                _registro_anomalia(
                    tipo="duplicidade_pdc",
                    detalhe=(
                        "A quantidade de registros da compra/entrada na T_MOVEST excede a quantidade "
                        "de lancamentos equivalentes na T_ITPDC para o mesmo numdoc/cdemp/cditem/qtde."
                    ),
                    data_mov=row.get("data_mov"),
                    numdoc=row.get("numdoc"),
                    cdemp=row.get("cdemp"),
                    cditem=row.get("cditem"),
                    qtde=row.get("qtde"),
                    especie=row.get("especie"),
                    st=row.get("st"),
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=row.get("sldantemp_atual"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

    if not df_duplicidades_transf.empty:
        for row in df_duplicidades_transf.to_dict("records"):
            anomalias.append(
                _registro_anomalia(
                    tipo="duplicidade_transferencia",
                    detalhe=(
                        "A quantidade de registros da transferencia na T_MOVEST excede a quantidade "
                        "de lancamentos equivalentes na T_ITTRANSF para o mesmo numdoc/cdemp/cditem/qtde/especie/st."
                    ),
                    data_mov=row.get("data_mov"),
                    numdoc=row.get("numdoc"),
                    cdemp=row.get("cdemp"),
                    cditem=row.get("cditem"),
                    qtde=row.get("qtde"),
                    especie=row.get("especie"),
                    st=row.get("st"),
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=row.get("sldantemp_atual"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

    if not grupos_duplicados.empty:
        df_registros_duplicados = df_atual.merge(
            grupos_duplicados,
            on=CHAVE_AUDITORIA_COLUNAS,
            how="inner",
            suffixes=("", "_grupo"),
        )
        if nrlans_duplicidade_origem:
            df_registros_duplicados = df_registros_duplicados[
                ~df_registros_duplicados["nrlan_atual"].isin(nrlans_duplicidade_origem)
            ].copy()
        if nrlans_cancelamentos_venda_compensados:
            df_registros_duplicados = df_registros_duplicados[
                ~df_registros_duplicados["nrlan_atual"].isin(nrlans_cancelamentos_venda_compensados)
            ].copy()
        for row in df_registros_duplicados.to_dict("records"):
            anomalias.append(
                _registro_anomalia(
                    tipo="duplicidade_movimento",
                    detalhe=(
                        "A chave de negocio aparece mais de uma vez na T_MOVEST "
                        f"({row.get('qtd_encontrada_grupo')} ocorrencias)."
                    ),
                    data_mov=row.get("data_mov"),
                    numdoc=row.get("numdoc"),
                    cdemp=row.get("cdemp"),
                    cditem=row.get("cditem"),
                    qtde=row.get("qtde"),
                    especie=row.get("especie"),
                    st=row.get("st"),
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=row.get("sldantemp_atual"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

    for row in df_comparacao.to_dict("records"):
        data_mov = _primeiro_valor_preenchido(row.get("data_mov_atual"), row.get("data_mov_esperado"))
        numdoc = _primeiro_valor_preenchido(row.get("numdoc_atual"), row.get("numdoc_esperado"))
        cdemp = _primeiro_valor_preenchido(row.get("cdemp_atual"), row.get("cdemp_esperado"))
        cditem = _primeiro_valor_preenchido(row.get("cditem_atual"), row.get("cditem_esperado"))
        qtde = _primeiro_valor_preenchido(row.get("qtde_atual"), row.get("qtde_esperado"))
        especie = _primeiro_valor_preenchido(row.get("especie_atual"), row.get("especie_esperado"))
        st = _primeiro_valor_preenchido(row.get("st_atual"), row.get("st_esperado"))

        if row["_merge"] == "left_only":
            anomalias.append(
                _registro_anomalia(
                    tipo="movimento_nao_reconstruido",
                    detalhe=(
                        "O registro existe na T_MOVEST atual, mas nao foi encontrado "
                        "na simulacao reconstruida."
                    ),
                    data_mov=data_mov,
                    numdoc=numdoc,
                    cdemp=cdemp,
                    cditem=cditem,
                    qtde=qtde,
                    especie=especie,
                    st=st,
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=row.get("sldantemp_atual"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )
            continue

        if row["_merge"] == "right_only":
            anomalias.append(
                _registro_anomalia(
                    tipo="movimento_ausente",
                    detalhe=(
                        "O registro foi reconstruido na simulacao, mas nao existe "
                        "na T_MOVEST atual."
                    ),
                    data_mov=data_mov,
                    numdoc=numdoc,
                    cdemp=cdemp,
                    cditem=cditem,
                    qtde=qtde,
                    especie=especie,
                    st=st,
                    nrlan_esperado=row.get("nrlan_esperado"),
                    seqit_esperado=row.get("seqit_esperado"),
                    saldoant_esperado=row.get("saldoant_esperado"),
                    sldantemp_esperado=row.get("sldantemp_esperado"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                )
            )
            continue

        saldoant_atual = _valor_float(row.get("saldoant_atual"))
        saldoant_esperado = _valor_float(row.get("saldoant_esperado"))
        if (
            saldoant_atual is not None
            and saldoant_esperado is not None
            and abs(saldoant_atual - saldoant_esperado) > TOLERANCIA_AUDITORIA
        ):
            anomalias.append(
                _registro_anomalia(
                    tipo="saldoant_divergente",
                    detalhe="O saldoant gravado diverge do saldo anterior calculado pela simulacao.",
                    data_mov=data_mov,
                    numdoc=numdoc,
                    cdemp=cdemp,
                    cditem=cditem,
                    qtde=qtde,
                    especie=especie,
                    st=st,
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=saldoant_atual,
                    sldantemp_atual=row.get("sldantemp_atual"),
                    nrlan_esperado=row.get("nrlan_esperado"),
                    seqit_esperado=row.get("seqit_esperado"),
                    saldoant_esperado=saldoant_esperado,
                    sldantemp_esperado=row.get("sldantemp_esperado"),
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

        sldantemp_atual = _valor_float(row.get("sldantemp_atual"))
        sldantemp_esperado = _valor_float(row.get("sldantemp_esperado"))
        if (
            sldantemp_atual is not None
            and sldantemp_esperado is not None
            and abs(sldantemp_atual - sldantemp_esperado) > TOLERANCIA_AUDITORIA
        ):
            anomalias.append(
                _registro_anomalia(
                    tipo="sldantemp_divergente",
                    detalhe="O SldAntEmp gravado diverge do saldo anterior por empresa calculado pela simulacao.",
                    data_mov=data_mov,
                    numdoc=numdoc,
                    cdemp=cdemp,
                    cditem=cditem,
                    qtde=qtde,
                    especie=especie,
                    st=st,
                    nrlan_atual=row.get("nrlan_atual"),
                    seqit_atual=row.get("seqit_atual"),
                    saldoant_atual=row.get("saldoant_atual"),
                    sldantemp_atual=sldantemp_atual,
                    nrlan_esperado=row.get("nrlan_esperado"),
                    seqit_esperado=row.get("seqit_esperado"),
                    saldoant_esperado=row.get("saldoant_esperado"),
                    sldantemp_esperado=sldantemp_esperado,
                    qtd_encontrada_grupo=row.get("qtd_encontrada_grupo"),
                    qtd_esperada_grupo=row.get("qtd_esperada_grupo"),
                    nrlans_grupo=row.get("nrlans_grupo"),
                )
            )

    df_anomalias = pd.DataFrame(anomalias)
    if not df_anomalias.empty:
        ajustes_resumo = df_anomalias.apply(
            lambda row: pd.Series(
                _resumir_ajustes_posteriores(
                    df_ajustes,
                    row.get("data_mov"),
                    row.get("cditem"),
                    cdemp=row.get("cdemp"),
                    nrlan_referencia=row.get("nrlan_atual"),
                )
            ),
            axis=1,
        )
        df_anomalias = pd.concat([df_anomalias, ajustes_resumo], axis=1)

    if not df_anomalias.empty and codigo_empresa is not None:
        df_anomalias = df_anomalias[df_anomalias["cdemp"] == codigo_empresa].reset_index(drop=True)

    resumo = {
        "data_corte": data_corte,
        "codigo_item": codigo_item,
        "codigo_empresa": codigo_empresa,
        "tabela_inventario": tabela_inventario,
        "importar_ajuste_inventario": importar_ajuste_inventario,
        "qtd_movimentos_atuais_analisados": int(len(df_atual)),
        "qtd_movimentos_esperados": int(len(df_esperado)),
        "qtd_ajustes_inventario_lidos": int(len(df_ajustes)),
        "qtd_anomalias": int(len(df_anomalias)),
        "qtd_itens_com_anomalia": int(
            df_anomalias["cditem"].dropna().nunique()
            if not df_anomalias.empty and "cditem" in df_anomalias.columns
            else 0
        ),
        "qtd_tipos_anomalia": int(
            df_anomalias["tipo"].nunique() if not df_anomalias.empty and "tipo" in df_anomalias else 0
        ),
    }
    return df_anomalias, resumo


def resumir_anomalias_por_item(df_anomalias):
    colunas = [
        "cditem",
        "qtd_anomalias",
        "qtd_tipos_anomalia",
        "qtd_empresas_afetadas",
        "qtd_anomalias_com_ajuste_posterior",
        "tipos_anomalia",
    ]
    if df_anomalias.empty or "cditem" not in df_anomalias.columns:
        return pd.DataFrame(columns=colunas)

    df_base = df_anomalias[df_anomalias["cditem"].notna()].copy()
    if df_base.empty:
        return pd.DataFrame(columns=colunas)

    resumo = (
        df_base.groupby("cditem", dropna=False)
        .agg(
            qtd_anomalias=("cditem", "size"),
            qtd_tipos_anomalia=("tipo", "nunique"),
            qtd_empresas_afetadas=("cdemp", "nunique"),
            qtd_anomalias_com_ajuste_posterior=(
                "possui_ajuste_inventario_posterior",
                lambda serie: int(serie.fillna(False).astype(bool).sum()),
            ),
        )
        .reset_index()
    )

    tipos = (
        df_base.groupby("cditem", dropna=False)["tipo"]
        .apply(lambda serie: ", ".join(sorted({str(valor) for valor in serie.dropna().tolist()})))
        .reset_index(name="tipos_anomalia")
    )

    resumo = resumo.merge(tipos, on="cditem", how="left")
    return resumo.sort_values(by=["qtd_anomalias", "cditem"], ascending=[False, True]).reset_index(
        drop=True
    )


def auditar_movest(engine_base, engine_atual, data_corte, codigo_item=None, codigo_empresa=None):
    df_saldo_base = _carregar_saldoit(
        engine_base, codigo_item=codigo_item, codigo_empresa=codigo_empresa
    )
    df_saldo_atual = _carregar_saldoit(
        engine_atual, codigo_item=codigo_item, codigo_empresa=codigo_empresa
    )
    df_mov = _carregar_movest(
        engine_atual, data_corte, codigo_item=codigo_item, codigo_empresa=codigo_empresa
    )
    auditar_por_empresa = codigo_empresa is not None

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
            esperado_emp = float(saldo_calc_emp.get((cditem, cdemp), 0))

            if auditar_por_empresa:
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
            else:
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

            delta = _delta_movimento(float(row["qtde"]), row["st"])
            saldo_calc_item[cditem] = esperado_geral + delta
            saldo_calc_emp[(cditem, cdemp)] = esperado_emp + delta

    itens_auditados = set(saldo_base_item) | set(saldo_atual_item) | set(empresas_por_item)
    if codigo_item is not None:
        itens_auditados.add(codigo_item)

    for cditem in sorted(itens_auditados):
        if auditar_por_empresa:
            empresas_item = {
                emp for item, emp in saldo_base_emp if item == cditem
            } | {
                emp for item, emp in saldo_atual_emp if item == cditem
            } | empresas_por_item.get(cditem, set())

            for cdemp in sorted(empresas_item):
                esperado_emp = float(
                    saldo_calc_emp.get((cditem, cdemp), saldo_base_emp.get((cditem, cdemp), 0))
                )
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
        else:
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

    df_discrepancias = pd.DataFrame(discrepancias)
    resumo = {
        "data_corte": data_corte,
        "codigo_item": codigo_item,
        "codigo_empresa": codigo_empresa,
        "campo_auditado": "SldAntEmp" if auditar_por_empresa else "saldoant",
        "qtd_movimentos_auditados": int(len(df_mov)),
        "qtd_itens_auditados": int(len(itens_auditados)),
        "qtd_discrepancias": int(len(df_discrepancias)),
    }
    return df_discrepancias, resumo


def auditar_saldos_pos_update(
    engine_atual,
    data_corte,
    tabela_backup=None,
    codigo_item=None,
    codigo_empresa=None,
):
    df_saldo_atual = _carregar_saldoit(
        engine_atual,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
        tabela="t_saldoit",
    )
    df_ultimo_mov = _carregar_ultimo_saldo_movest(
        engine_atual,
        codigo_item=codigo_item,
        codigo_empresa=codigo_empresa,
    )

    saldo_atual_emp = (
        df_saldo_atual.set_index(["cditem", "cdemp", "empitem"])["saldo"].to_dict()
        if not df_saldo_atual.empty
        else {}
    )
    saldo_ultimo_emp = (
        df_ultimo_mov.set_index(["cditem", "cdemp", "empitem"])["saldo_final"].to_dict()
        if not df_ultimo_mov.empty
        else {}
    )
    detalhes_ultimo_emp = (
        df_ultimo_mov.set_index(["cditem", "cdemp", "empitem"])[
            ["nrlan_sort", "data_mov", "numdoc", "seqit_sort", "qtde", "st", "sldantemp_ultimo"]
        ].to_dict("index")
        if not df_ultimo_mov.empty
        else {}
    )

    pares_auditados = set(saldo_ultimo_emp) | set(saldo_atual_emp)
    discrepancias = []

    for cditem, cdemp, empitem in sorted(pares_auditados):
        chave = (cditem, cdemp, empitem)
        encontrado = float(saldo_atual_emp.get(chave, 0))
        detalhe_mov = detalhes_ultimo_emp.get(chave, {})
        if chave in saldo_ultimo_emp:
            esperado = float(saldo_ultimo_emp[chave])
            detalhe = (
                "Saldo em t_saldoit divergente do saldo final esperado a partir do ultimo "
                "nrlan da T_MOVEST por cditem/cdemp/empitem, calculado com SldAntEmp +/- qtde."
            )
        else:
            esperado = 0.0
            detalhe = (
                "Nao existe trio cditem/cdemp/empitem correspondente na T_MOVEST para este "
                "registro de t_saldoit; neste caso o saldo esperado e zero."
            )
        if abs(encontrado - esperado) > 0.000001:
            _add_discrepancia(
                discrepancias,
                "saldo_t_saldoit_vs_ultimo_nrlan_cditem_cdemp_empitem",
                cditem,
                esperado,
                encontrado,
                cdemp=cdemp,
                empitem=empitem,
                saldo_backup=None,
                nrlan=detalhe_mov.get("nrlan_sort"),
                data_mov=detalhe_mov.get("data_mov"),
                numdoc=detalhe_mov.get("numdoc"),
                seqit=detalhe_mov.get("seqit_sort"),
                qtde=detalhe_mov.get("qtde"),
                st=detalhe_mov.get("st"),
                sldantemp_ultimo=detalhe_mov.get("sldantemp_ultimo"),
                detalhe=detalhe,
            )

    df_discrepancias = pd.DataFrame(discrepancias)
    resumo = {
        "data_corte": data_corte,
        "codigo_item": codigo_item,
        "codigo_empresa": codigo_empresa,
        "criterio_referencia": "ultimo_nrlan_t_movest_por_cditem_cdemp_empitem",
        "qtd_movimentos_auditados": int(len(df_ultimo_mov)),
        "qtd_itens_auditados": int(len({cditem for cditem, _, _ in pares_auditados})),
        "qtd_pares_item_empresa_auditados": int(len(pares_auditados)),
        "qtd_discrepancias": int(len(df_discrepancias)),
    }
    return df_discrepancias, resumo


def salvar_relatorio_auditoria(df_discrepancias, codigo_item=None, codigo_empresa=None):
    pasta_saida = Path("relatorios_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo_item = f"_item_{codigo_item}" if codigo_item is not None else "_todos"
    sufixo_empresa = f"_emp_{codigo_empresa}" if codigo_empresa is not None else ""
    caminho_saida = pasta_saida / f"auditoria_movest{sufixo_item}{sufixo_empresa}_{timestamp}.csv"

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


def salvar_relatorio_anomalias_movest(df_anomalias, codigo_item=None, codigo_empresa=None):
    pasta_saida = Path("relatorios_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo_item = f"_item_{codigo_item}" if codigo_item is not None else "_todos"
    sufixo_empresa = f"_emp_{codigo_empresa}" if codigo_empresa is not None else ""
    caminho_saida = (
        pasta_saida / f"auditoria_movest_anomalias{sufixo_item}{sufixo_empresa}_{timestamp}.csv"
    )

    colunas = [
        "tipo",
        "detalhe",
        "chave_negocio",
        "data_mov",
        "numdoc",
        "cdemp",
        "cditem",
        "qtde",
        "especie",
        "st",
        "nrlan_atual",
        "seqit_atual",
        "saldoant_atual",
        "sldantemp_atual",
        "nrlan_esperado",
        "seqit_esperado",
        "saldoant_esperado",
        "sldantemp_esperado",
        "diferenca_saldoant",
        "diferenca_sldantemp",
        "qtd_encontrada_grupo",
        "qtd_esperada_grupo",
        "nrlans_grupo",
        "possui_ajuste_inventario_posterior",
        "qtd_ajustes_inventario_posteriores",
        "primeiro_ajuste_inventario_posterior",
        "ultimo_ajuste_inventario_posterior",
        "ajustes_inventario_posteriores",
    ]
    if df_anomalias.empty:
        pd.DataFrame(columns=colunas).to_csv(caminho_saida, index=False, encoding="utf-8-sig")
    else:
        colunas_saida = [col for col in colunas if col in df_anomalias.columns]
        df_anomalias[colunas_saida].to_csv(caminho_saida, index=False, encoding="utf-8-sig")

    return caminho_saida


def salvar_relatorio_resumo_anomalias_por_item(
    df_resumo_itens,
    codigo_item=None,
    codigo_empresa=None,
):
    pasta_saida = Path("relatorios_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo_item = f"_item_{codigo_item}" if codigo_item is not None else "_todos"
    sufixo_empresa = f"_emp_{codigo_empresa}" if codigo_empresa is not None else ""
    caminho_saida = (
        pasta_saida
        / f"auditoria_movest_anomalias_resumo_itens{sufixo_item}{sufixo_empresa}_{timestamp}.csv"
    )

    colunas = [
        "cditem",
        "qtd_anomalias",
        "qtd_tipos_anomalia",
        "qtd_empresas_afetadas",
        "qtd_anomalias_com_ajuste_posterior",
        "tipos_anomalia",
    ]
    if df_resumo_itens.empty:
        pd.DataFrame(columns=colunas).to_csv(caminho_saida, index=False, encoding="utf-8-sig")
    else:
        colunas_saida = [col for col in colunas if col in df_resumo_itens.columns]
        df_resumo_itens[colunas_saida].to_csv(caminho_saida, index=False, encoding="utf-8-sig")

    return caminho_saida


def salvar_relatorio_auditoria_saldoit(
    df_discrepancias,
    tabela_backup=None,
    codigo_item=None,
    codigo_empresa=None,
):
    pasta_saida = Path("relatorios_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sufixo_item = f"_item_{codigo_item}" if codigo_item is not None else "_todos"
    sufixo_empresa = f"_emp_{codigo_empresa}" if codigo_empresa is not None else ""
    tabela_backup_slug = str(tabela_backup or "ultimo_nrlan_t_movest_cditem_cdemp_empitem").replace(".", "_")
    caminho_saida = (
        pasta_saida
        / f"auditoria_t_saldoit_{tabela_backup_slug}{sufixo_item}{sufixo_empresa}_{timestamp}.csv"
    )

    colunas = [
        "tipo",
        "cditem",
        "cdemp",
        "empitem",
        "data_mov",
        "numdoc",
        "nrlan",
        "seqit",
        "st",
        "qtde",
        "sldantemp_ultimo",
        "saldo_backup",
        "esperado",
        "encontrado",
        "diferenca",
        "detalhe",
    ]
    if df_discrepancias.empty:
        pd.DataFrame(columns=colunas).to_csv(caminho_saida, index=False, encoding="utf-8-sig")
    else:
        colunas_saida = [col for col in colunas if col in df_discrepancias.columns]
        df_discrepancias[colunas_saida].to_csv(
            caminho_saida, index=False, encoding="utf-8-sig"
        )

    return caminho_saida
