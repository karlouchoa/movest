from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.database import get_engine
from src.transform import extrair_movimentacoes_novas
from src.utils import atualizar_saldos_finais, recriar_indices


def solicitar_nomes_bancos():
    banco_base = input("Informe o nome do banco base (origem): ").strip()
    banco_atual = input("Informe o nome do banco atual (destino): ").strip()

    if not banco_base:
        raise ValueError("O nome do banco base nao pode ficar vazio.")
    if not banco_atual:
        raise ValueError("O nome do banco atual nao pode ficar vazio.")

    return banco_base, banco_atual


def validar_conexao(engine, nome_banco, papel):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise RuntimeError(
            f"Nao foi possivel conectar ao banco {papel} '{nome_banco}'. "
            "Verifique o nome informado, a permissao do usuario e se o banco existe no servidor."
        ) from exc


def preparar_t_movest_destino(engine_base, engine_atual):
    tabela_inventario = None

    with engine_atual.begin() as conn:
        existe = conn.execute(
            text(
                """
                SELECT 1
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = 'dbo' AND t.name = 'T_MOVEST'
                """
            )
        ).scalar()

        if existe:
            sufixo = datetime.now().strftime("%Y%m%d_%H%M%S")
            tabela_inventario = f"T_MOVEST_INV_{sufixo}"
            conn.execute(text(f"EXEC sp_rename 'dbo.T_MOVEST', '{tabela_inventario}'"))

        base_db = (engine_base.url.database or "Bancobase").replace("]", "]]")
        conn.execute(text(f"SELECT * INTO dbo.T_MOVEST FROM [{base_db}].dbo.T_MOVEST"))
        return tabela_inventario


def obter_data_corte_base(engine_base):
    with engine_base.connect() as conn:
        data_corte = conn.execute(text("SELECT MAX(DataLan) FROM T_MOVEST")).scalar()
    return data_corte or datetime(1900, 1, 1)


def preparar_colunas_para_insert(df_novos, colunas_destino):
    if "clifor" in colunas_destino:
        if "clifor" not in df_novos.columns:
            df_novos["clifor"] = 1
        else:
            df_novos["clifor"] = df_novos["clifor"].fillna(1)

    if "datadoc" in colunas_destino and "datadoc" not in df_novos.columns:
        df_novos["datadoc"] = df_novos["data"]

    if "DataLan" in colunas_destino and "DataLan" not in df_novos.columns:
        df_novos["DataLan"] = df_novos["data"]

    if "data" in colunas_destino and "data" not in df_novos.columns and "DataLan" in df_novos.columns:
        df_novos["data"] = df_novos["DataLan"]

    if "empmov" in colunas_destino and "empmov" not in df_novos.columns and "cdemp" in df_novos.columns:
        df_novos["empmov"] = df_novos["cdemp"]

    if "empitem" in colunas_destino:
        df_novos["empitem"] = 1

    if "empfor" in colunas_destino:
        if "empfor" not in df_novos.columns:
            df_novos["empfor"] = 1
        else:
            df_novos["empfor"] = df_novos["empfor"].fillna(1)

    if "empven" in colunas_destino:
        if "empven" not in df_novos.columns:
            df_novos["empven"] = df_novos["cdemp"]
        else:
            df_novos["empven"] = df_novos["empven"].fillna(df_novos["cdemp"])

    if "obsit" in colunas_destino:
        if "obsit" not in df_novos.columns:
            df_novos["obsit"] = df_novos["obs"]
        else:
            df_novos["obsit"] = df_novos["obsit"].fillna(df_novos["obs"])

    if "Preco" in colunas_destino and "Preco" not in df_novos.columns:
        df_novos["Preco"] = 0
    if "Desconto" in colunas_destino and "Desconto" not in df_novos.columns:
        df_novos["Desconto"] = 0
    if "custo" in colunas_destino and "custo" not in df_novos.columns:
        df_novos["custo"] = 0
    if "COMPRA" in colunas_destino and "COMPRA" not in df_novos.columns:
        df_novos["COMPRA"] = 0

    if "isdeleted" in colunas_destino:
        if "isdeleted" not in df_novos.columns:
            df_novos["isdeleted"] = 0
        else:
            df_novos["isdeleted"] = df_novos["isdeleted"].fillna(0)

    agora = datetime.now()
    if "createdat" in colunas_destino and "createdat" not in df_novos.columns:
        df_novos["createdat"] = agora
    if "updatedat" in colunas_destino and "updatedat" not in df_novos.columns:
        df_novos["updatedat"] = agora

    return df_novos


def preencher_nrlan(df_novos, conn, colunas_destino):
    if "nrlan" not in colunas_destino:
        return df_novos

    nrlan_is_identity = conn.execute(
        text("SELECT COLUMNPROPERTY(OBJECT_ID('dbo.T_MOVEST'), 'nrlan', 'IsIdentity')")
    ).scalar()
    if int(nrlan_is_identity or 0) == 1:
        if "nrlan" in df_novos.columns:
            return df_novos.drop(columns=["nrlan"])
        return df_novos

    if "nrlan" in df_novos.columns and df_novos["nrlan"].notna().all():
        return df_novos

    max_nrlan = conn.execute(text("SELECT ISNULL(MAX(nrlan), 0) FROM T_MOVEST")).scalar() or 0
    qtd = len(df_novos)
    df_novos["nrlan"] = range(int(max_nrlan) + 1, int(max_nrlan) + 1 + qtd)
    return df_novos


def normalizar_tipos_para_insert(df_para_gravar, conn):
    tipos = conn.execute(
        text(
            """
            SELECT c.name AS column_name, TYPE_NAME(c.user_type_id) AS type_name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('dbo.T_MOVEST')
            """
        )
    ).fetchall()
    mapa_tipos = {r._mapping["column_name"]: r._mapping["type_name"].lower() for r in tipos}

    tipos_int = {"int", "bigint", "smallint", "tinyint"}
    for col in list(df_para_gravar.columns):
        tipo = mapa_tipos.get(col)
        if tipo in tipos_int:
            serie = df_para_gravar[col].astype("string").str.strip()
            serie = serie.str.replace(r"[^0-9-]", "", regex=True)
            serie = serie.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
            nums = pd.to_numeric(serie, errors="coerce")
            df_para_gravar[col] = nums.apply(lambda x: int(x) if pd.notna(x) else None)

        if tipo == "bit":
            nums = pd.to_numeric(df_para_gravar[col], errors="coerce").fillna(0)
            df_para_gravar[col] = nums.apply(lambda x: 1 if int(x) != 0 else 0)

    return df_para_gravar


def calcular_delta(qtde, st):
    if pd.isna(qtde):
        return 0

    st_upper = str(st).upper()
    if st_upper == "E":
        return qtde
    if st_upper == "S":
        return -qtde
    return 0


def main():
    banco_base, banco_atual = solicitar_nomes_bancos()
    engine_base = get_engine(banco_base)
    engine_atual = get_engine(banco_atual)
    validar_conexao(engine_base, banco_base, "base")
    validar_conexao(engine_atual, banco_atual, "atual")

    print("1) Preparando T_MOVEST no Bancoatual...")
    tabela_inventario = preparar_t_movest_destino(engine_base, engine_atual)
    if tabela_inventario:
        print(f"T_MOVEST existente renomeada para {tabela_inventario}.")
        print("Nova T_MOVEST criada a partir do Bancobase.")
    else:
        print("T_MOVEST nao existia. Nova T_MOVEST criada a partir do Bancobase.")

    print("2) Lendo data inicial no Bancobase.T_MOVEST...")
    data_corte = obter_data_corte_base(engine_base)
    print(f"Data de corte: {data_corte}")

    print("3) Carregando saldos iniciais do Bancobase.t_saldoit...")
    df_s_init = pd.read_sql("SELECT cditem, cdemp, saldo FROM t_saldoit", engine_base)
    dict_geral = df_s_init.groupby("cditem")["saldo"].sum().to_dict()
    dict_emp = df_s_init.set_index(["cditem", "cdemp"])["saldo"].to_dict()

    print("4) Extraindo movimentacoes do Bancoatual...")
    df_novos = extrair_movimentacoes_novas(
        engine_atual,
        data_corte,
        tabela_inventario=tabela_inventario,
    )
    if df_novos.empty:
        print("Nenhuma movimentacao encontrada para inserir.")
        return

    colunas_ordenacao = ["data"]
    if "_ordem" in df_novos.columns:
        colunas_ordenacao.append("_ordem")
    colunas_ordenacao.append("numdoc")
    df_novos = df_novos.sort_values(by=colunas_ordenacao).reset_index(drop=True)
    print(f"Registros encontrados: {len(df_novos)}")

    print("5) Calculando saldoant e SldAntEmp...")
    saldo_ant_geral = []
    saldo_ant_emp = []
    saldos_finais_item_emp = {}

    for _, row in df_novos.iterrows():
        cditem, cdemp = row["cditem"], row["cdemp"]
        saldo_item = dict_geral.get(cditem, 0)
        saldo_item_emp = dict_emp.get((cditem, cdemp), 0)

        saldo_ant_geral.append(saldo_item)
        saldo_ant_emp.append(saldo_item_emp)

        delta = calcular_delta(row["qtde"], row["st"])

        dict_geral[cditem] = saldo_item + delta
        dict_emp[(cditem, cdemp)] = saldo_item_emp + delta
        saldos_finais_item_emp[(cditem, cdemp)] = dict_emp[(cditem, cdemp)]

    df_novos["saldoant"] = saldo_ant_geral
    df_novos["SldAntEmp"] = saldo_ant_emp

    print("6) Gravando em T_MOVEST e atualizando t_saldoit...")
    with engine_atual.begin() as conn:
        res_cols = conn.execute(text("SELECT TOP 0 * FROM T_MOVEST"))
        colunas_destino = list(res_cols.keys())

        df_novos = preparar_colunas_para_insert(df_novos, colunas_destino)
        df_novos = preencher_nrlan(df_novos, conn, colunas_destino)
        df_para_gravar = df_novos[[c for c in df_novos.columns if c in colunas_destino]]
        df_para_gravar = normalizar_tipos_para_insert(df_para_gravar, conn)
        df_para_gravar.to_sql("T_MOVEST", conn, if_exists="append", index=False, chunksize=1000)

        atualizar_saldos_finais(conn, saldos_finais_item_emp)

    print("7) Recriando indices de T_MOVEST...")
    recriar_indices(engine_atual)

    print("Concluido com sucesso.")


if __name__ == "__main__":
    main()
