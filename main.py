from datetime import datetime
import os

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

from src.database import get_engine
from src.transform import extrair_movimentacoes_novas
from src.utils import (
    atualizar_saldos_finais,
    criar_copia_seguranca_t_saldoit,
    replicar_estrutura_t_movest,
)


def solicitar_confirmacao(mensagem, padrao=False):
    sufixo = "[S/n]" if padrao else "[s/N]"
    resposta = input(f"{mensagem} {sufixo}: ").strip().lower()
    if not resposta:
        return padrao
    if resposta in {"s", "sim", "y", "yes"}:
        return True
    if resposta in {"n", "nao", "não", "no"}:
        return False
    raise ValueError("Resposta invalida. Informe S para sim ou N para nao.")


def solicitar_parametros_conexao(perguntar_importa_ajuste_inventario=False):
    load_dotenv()

    servidor = os.getenv("DB_SERVER", "").strip()
    if not servidor:
        servidor = input("Informe o servidor/instancia SQL [localhost]: ").strip() or "localhost"

    banco_base = os.getenv("DB_BASE", "").strip()
    if not banco_base:
        banco_base = input("Informe o nome do banco base (origem): ").strip()

    banco_atual = os.getenv("DB_ATUAL", "").strip()
    if not banco_atual:
        banco_atual = input("Informe o nome do banco atual (destino): ").strip()

    username = os.getenv("DB_USER", "").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    codigo_item_raw = input("Informe o codigo do item [todos]: ").strip()

    if not banco_base:
        raise ValueError("O nome do banco base nao pode ficar vazio.")
    if not banco_atual:
        raise ValueError("O nome do banco atual nao pode ficar vazio.")

    codigo_item = None
    if codigo_item_raw:
        if not codigo_item_raw.isdigit() or int(codigo_item_raw) <= 0:
            raise ValueError("O codigo do item deve ser um numero inteiro maior que zero.")
        codigo_item = int(codigo_item_raw)

    importa_ajuste_inventario = True
    if perguntar_importa_ajuste_inventario:
        importa_ajuste_inventario = solicitar_confirmacao(
            "Deseja importar movimentacoes de Ajuste de Inventario?",
            padrao=False,
        )

    return (
        servidor,
        banco_base,
        banco_atual,
        username,
        password,
        codigo_item,
        importa_ajuste_inventario,
    )


def validar_conexao(engine, nome_banco, papel, servidor):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise RuntimeError(
            f"Nao foi possivel conectar ao banco {papel} '{nome_banco}' no servidor '{servidor}'. "
            "Verifique o nome da instancia, o nome do banco, a permissao do usuario e se o SQL Server aceita conexoes."
        ) from exc


def preparar_t_movest_destino(engine_base, engine_atual, codigo_item=None):
    tabela_inventario = None

    with engine_base.connect() as conn_base:
        base_db = conn_base.execute(text("SELECT DB_NAME()")).scalar()
        existe_origem = conn_base.execute(
            text(
                """
                SELECT 1
                FROM sys.tables t
                JOIN sys.schemas s ON s.schema_id = t.schema_id
                WHERE s.name = 'dbo' AND t.name = 'T_MOVEST'
                """
            )
        ).scalar()

    if not base_db:
        raise RuntimeError("Nao foi possivel identificar o nome do banco base conectado.")
    if not existe_origem:
        raise RuntimeError(
            f"A tabela dbo.T_MOVEST nao foi encontrada no banco base '{base_db}'."
        )

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

        if codigo_item is not None:
            if not existe:
                raise RuntimeError(
                    "Para processar um item especifico, a dbo.T_MOVEST precisa existir no banco atual."
                )
            return "T_MOVEST"

        if existe:
            sufixo = datetime.now().strftime("%Y%m%d_%H%M%S")
            tabela_inventario = f"T_MOVEST_INV_{sufixo}"
            conn.execute(text(f"EXEC sp_rename 'dbo.T_MOVEST', '{tabela_inventario}'"))

        base_db_escaped = str(base_db).replace("]", "]]")
        conn.execute(text(f"SELECT * INTO dbo.T_MOVEST FROM [{base_db_escaped}].dbo.T_MOVEST"))
        return tabela_inventario


def obter_data_corte_base(engine_base):
    with engine_base.connect() as conn:
        if conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'datadoc')")).scalar():
            coluna_data_base = "datadoc"
        elif conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'DataLan')")).scalar():
            coluna_data_base = "DataLan"
        elif conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'data')")).scalar():
            coluna_data_base = "[data]"
        else:
            raise RuntimeError("A dbo.T_MOVEST nao possui DATADOC, DataLan ou data para definir a data base.")

        data_maxima_original = conn.execute(
            text(f"SELECT MAX({coluna_data_base}) FROM T_MOVEST")
        ).scalar()
        data_maxima_valida = conn.execute(
            text(f"SELECT MAX({coluna_data_base}) FROM T_MOVEST WHERE {coluna_data_base} <= GETDATE()")
        ).scalar()
    return (data_maxima_valida or datetime(1900, 1, 1)), data_maxima_original, coluna_data_base


def excluir_movest_por_item(conn, codigo_item, data_corte):
    coluna_data = "DataLan"
    possui_datalan = conn.execute(
        text("SELECT COL_LENGTH('dbo.T_MOVEST', 'DataLan')")
    ).scalar()
    if not possui_datalan:
        possui_data = conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'data')")).scalar()
        if possui_data:
            coluna_data = "[data]"
        else:
            raise RuntimeError("A dbo.T_MOVEST nao possui coluna de data valida para exclusao.")

    resultado = conn.execute(
        text(
            f"""
            DELETE FROM dbo.T_MOVEST
            WHERE cditem = :codigo_item
              AND {coluna_data} >= :data_corte
            """
        ),
        {"codigo_item": codigo_item, "data_corte": data_corte},
    )
    return int(resultado.rowcount or 0)


def revisar_clifor_entradas(conn, codigo_item=None):
    requisitos = [
        ("T_MOVEST", "clifor"),
        ("T_MOVEST", "st"),
        ("T_MOVEST", "cditem"),
        ("T_ITENS", "cditem"),
        ("T_ITENS", "cdfor"),
    ]
    for tabela, coluna in requisitos:
        if not conn.execute(text("SELECT COL_LENGTH(:tabela, :coluna)"), {"tabela": f"dbo.{tabela}", "coluna": coluna}).scalar():
            return 0

    filtro_item = ""
    params = {}
    if codigo_item is not None:
        filtro_item = " AND m.cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    resultado = conn.execute(
        text(
            f"""
            UPDATE m
            SET m.clifor = i.cdfor
            FROM dbo.T_MOVEST m
            INNER JOIN dbo.T_ITENS i
                ON i.cditem = m.cditem
            WHERE m.st = 'E'
              AND ISNULL(i.cdfor, 0) <> ISNULL(m.clifor, 0)
              {filtro_item}
            """
        ),
        params,
    )
    return int(resultado.rowcount or 0)


def preparar_colunas_para_insert(df_novos, colunas_destino):
    mapa_destino = {str(col).lower(): str(col) for col in colunas_destino}

    def nome_destino(nome_canonico):
        return mapa_destino.get(nome_canonico.lower(), nome_canonico)

    coluna_data = nome_destino("data")
    coluna_datalan = nome_destino("DataLan")
    coluna_datadoc = nome_destino("datadoc")

    if "data" in df_novos.columns and coluna_datalan in colunas_destino:
        df_novos[coluna_datalan] = df_novos["data"]

    if nome_destino("clifor") in colunas_destino:
        if "clifor" not in df_novos.columns:
            df_novos["clifor"] = 1
        else:
            df_novos.loc[df_novos["clifor"].isna(), "clifor"] = 1

    if coluna_datadoc in colunas_destino and coluna_datadoc not in df_novos.columns:
        df_novos[coluna_datadoc] = df_novos["data"]

    if coluna_datalan in colunas_destino and coluna_datalan not in df_novos.columns:
        df_novos[coluna_datalan] = df_novos["data"]

    if coluna_data in colunas_destino and coluna_data not in df_novos.columns and coluna_datalan in df_novos.columns:
        df_novos[coluna_data] = df_novos[coluna_datalan]

    renomear = {}
    for col in list(df_novos.columns):
        destino = mapa_destino.get(str(col).lower())
        if destino and destino != col:
            renomear[col] = destino
    if renomear:
        df_novos = df_novos.rename(columns=renomear)

    if nome_destino("empmov") in colunas_destino and nome_destino("empmov") not in df_novos.columns and "cdemp" in df_novos.columns:
        df_novos[nome_destino("empmov")] = df_novos["cdemp"]

    if nome_destino("empitem") in colunas_destino:
        if nome_destino("empitem") not in df_novos.columns:
            df_novos[nome_destino("empitem")] = 1
        else:
            df_novos.loc[df_novos[nome_destino("empitem")].isna(), nome_destino("empitem")] = 1

    if nome_destino("empfor") in colunas_destino:
        if nome_destino("empfor") not in df_novos.columns:
            df_novos[nome_destino("empfor")] = 1
        else:
            df_novos.loc[df_novos[nome_destino("empfor")].isna(), nome_destino("empfor")] = 1

    if nome_destino("empven") in colunas_destino:
        if nome_destino("empven") not in df_novos.columns:
            df_novos[nome_destino("empven")] = df_novos["cdemp"]
        else:
            df_novos.loc[df_novos[nome_destino("empven")].isna(), nome_destino("empven")] = df_novos.loc[
                df_novos[nome_destino("empven")].isna(), "cdemp"
            ]

    if nome_destino("obsit") in colunas_destino:
        if nome_destino("obsit") not in df_novos.columns:
            df_novos[nome_destino("obsit")] = df_novos["obs"]
        else:
            df_novos.loc[df_novos[nome_destino("obsit")].isna(), nome_destino("obsit")] = df_novos.loc[
                df_novos[nome_destino("obsit")].isna(), "obs"
            ]

    for coluna_zero in ("Preco", "valor", "Valor", "Desconto", "custo", "COMPRA"):
        destino = nome_destino(coluna_zero)
        if destino in colunas_destino and destino not in df_novos.columns:
            df_novos[destino] = 0

    if nome_destino("isdeleted") in colunas_destino:
        if nome_destino("isdeleted") not in df_novos.columns:
            df_novos[nome_destino("isdeleted")] = 0
        else:
            df_novos.loc[df_novos[nome_destino("isdeleted")].isna(), nome_destino("isdeleted")] = 0

    agora = datetime.now()
    if nome_destino("createdat") in colunas_destino and nome_destino("createdat") not in df_novos.columns:
        df_novos[nome_destino("createdat")] = agora
    if nome_destino("updatedat") in colunas_destino and nome_destino("updatedat") not in df_novos.columns:
        df_novos[nome_destino("updatedat")] = agora

    grupos = {}
    for col in df_novos.columns:
        grupos.setdefault(str(col).lower(), []).append(col)

    consolidado = {}
    ordem_colunas = []
    for chave, nomes in grupos.items():
        nome_final = mapa_destino.get(chave, nomes[0])
        if len(nomes) == 1:
            consolidado[nome_final] = df_novos[nomes[0]]
        else:
            consolidado[nome_final] = df_novos[nomes].bfill(axis=1).iloc[:, 0]
        ordem_colunas.append(nome_final)

    df_novos = pd.DataFrame(consolidado)
    df_novos = df_novos.loc[:, ordem_colunas]
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


def validar_datalan_igual_data(df_para_gravar):
    mapa = {str(col).lower(): str(col) for col in df_para_gravar.columns}
    coluna_data = mapa.get("data")
    coluna_datalan = mapa.get("datalan")
    if not coluna_data or not coluna_datalan:
        return

    serie_data = pd.to_datetime(df_para_gravar.loc[:, coluna_data], errors="coerce")
    if isinstance(serie_data, pd.DataFrame):
        serie_data = serie_data.bfill(axis=1).iloc[:, 0]

    serie_datalan = pd.to_datetime(df_para_gravar.loc[:, coluna_datalan], errors="coerce")
    if isinstance(serie_datalan, pd.DataFrame):
        serie_datalan = serie_datalan.bfill(axis=1).iloc[:, 0]

    divergentes = ~(
        (serie_data == serie_datalan)
        | (serie_data.isna() & serie_datalan.isna())
    )

    if divergentes.any():
        colunas_exemplo = [c for c in ("numdoc", "especie", "st") if c in df_para_gravar.columns]
        colunas_exemplo.extend([coluna_data, coluna_datalan])
        exemplo = df_para_gravar.loc[divergentes, colunas_exemplo].head(10)
        raise RuntimeError(
            "Foram encontrados registros com DataLan diferente de data antes da insercao em T_MOVEST.\n"
            + exemplo.to_string(index=False)
        )


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
    (
        servidor,
        banco_base,
        banco_atual,
        username,
        password,
        codigo_item,
        importa_ajuste_inventario,
    ) = solicitar_parametros_conexao(perguntar_importa_ajuste_inventario=True)
    engine_base = get_engine(servidor, banco_base, username, password)
    engine_atual = get_engine(servidor, banco_atual, username, password)
    validar_conexao(engine_base, banco_base, "base", servidor)
    validar_conexao(engine_atual, banco_atual, "atual", servidor)

    print("1) Lendo data inicial no Bancobase.T_MOVEST...")
    data_corte, data_maxima_base, coluna_data_base = obter_data_corte_base(engine_base)
    if data_maxima_base and data_maxima_base > datetime.now():
        print(
            f"Data maxima futura detectada em T_MOVEST.{coluna_data_base} ({data_maxima_base}). "
            f"Usando a maior data valida ate hoje: {data_corte}"
        )
    else:
        print(f"Data maxima em T_MOVEST.{coluna_data_base}: {data_maxima_base}")
    print(f"Data de corte: {data_corte}")

    print("2) Preparando T_MOVEST no Bancoatual...")
    tabela_inventario = preparar_t_movest_destino(engine_base, engine_atual, codigo_item=codigo_item)
    if codigo_item is not None:
        print("Processamento por item: a T_MOVEST atual sera mantida e usada como origem de leitura.")
    elif tabela_inventario:
        print(f"T_MOVEST existente renomeada para {tabela_inventario}.")
        print("Nova T_MOVEST criada a partir do Bancobase.")
    else:
        print("T_MOVEST nao existia. Nova T_MOVEST criada a partir do Bancobase.")

    if codigo_item:
        print(f"Filtro de processamento: apenas item {codigo_item}.")
    else:
        print("Filtro de processamento: todos os itens.")
    if importa_ajuste_inventario:
        print("Importacao de ajustes de inventario: habilitada.")
    else:
        print("Importacao de ajustes de inventario: desabilitada.")

    print("3) Carregando saldos iniciais do Bancobase.t_saldoit...")
    q_saldoit = "SELECT cditem, cdemp, saldo FROM t_saldoit"
    params_saldoit = None
    if codigo_item:
        q_saldoit += " WHERE cditem = :codigo_item"
        params_saldoit = {"codigo_item": codigo_item}
    df_s_init = pd.read_sql(text(q_saldoit), engine_base, params=params_saldoit)
    dict_geral = df_s_init.groupby("cditem")["saldo"].sum().to_dict()
    dict_emp = df_s_init.set_index(["cditem", "cdemp"])["saldo"].to_dict()

    print("4) Extraindo movimentacoes do Bancoatual...")
    df_novos = extrair_movimentacoes_novas(
        engine_atual,
        data_corte,
        tabela_inventario=tabela_inventario,
        codigo_item=codigo_item,
        importar_ajuste_inventario=importa_ajuste_inventario,
    )

    coluna_ordenacao_base = "DataLan" if "DataLan" in df_novos.columns else "data"
    colunas_ordenacao = [coluna_ordenacao_base]
    if "_nrlan_origem" in df_novos.columns:
        df_novos["_nrlan_origem"] = pd.to_numeric(df_novos["_nrlan_origem"], errors="coerce")
        colunas_ordenacao.append("_nrlan_origem")
    if "_ordem" in df_novos.columns:
        colunas_ordenacao.append("_ordem")
    colunas_ordenacao.append("numdoc")
    if "SEQIT" in df_novos.columns:
        df_novos["_seqit_sort"] = pd.to_numeric(df_novos["SEQIT"], errors="coerce").fillna(0)
        colunas_ordenacao.append("_seqit_sort")
    df_novos = df_novos.sort_values(by=colunas_ordenacao).reset_index(drop=True)
    print(f"Registros encontrados: {len(df_novos)}")

    print("5) Calculando saldoant e SldAntEmp...")
    saldo_ant_geral = []
    saldo_ant_emp = []

    for _, row in df_novos.iterrows():
        cditem, cdemp = row["cditem"], row["cdemp"]
        saldo_item = dict_geral.get(cditem, 0)
        saldo_item_emp = dict_emp.get((cditem, cdemp), 0)

        saldo_ant_geral.append(saldo_item)
        saldo_ant_emp.append(saldo_item_emp)

        delta = calcular_delta(row["qtde"], row["st"])

        dict_geral[cditem] = saldo_item + delta
        dict_emp[(cditem, cdemp)] = saldo_item_emp + delta
    df_novos["saldoant"] = saldo_ant_geral
    df_novos["SldAntEmp"] = saldo_ant_emp

    print("6) Gravando em T_MOVEST e atualizando t_saldoit...")
    tabela_backup_saldoit = None
    with engine_atual.begin() as conn:
        if codigo_item is not None:
            qtd_excluida = excluir_movest_por_item(conn, codigo_item, data_corte)
            print(f"   Excluindo registros existentes do item {codigo_item} em T_MOVEST desde {data_corte}...")
            print(f"   Registros excluidos: {qtd_excluida}")

        res_cols = conn.execute(text("SELECT TOP 0 * FROM T_MOVEST"))
        colunas_destino = list(res_cols.keys())

        if not df_novos.empty:
            print("   Preparando colunas para insercao...")
            df_novos = preparar_colunas_para_insert(df_novos, colunas_destino)
            df_novos = preencher_nrlan(df_novos, conn, colunas_destino)
            df_para_gravar = df_novos[[c for c in df_novos.columns if c in colunas_destino]].copy()
            df_para_gravar = normalizar_tipos_para_insert(df_para_gravar, conn)
            validar_datalan_igual_data(df_para_gravar)
            print(f"   Inserindo {len(df_para_gravar)} registros em T_MOVEST...")
            df_para_gravar.to_sql("T_MOVEST", conn, if_exists="append", index=False, chunksize=1000)
            print("   Insercao em T_MOVEST concluida.")
        else:
            print("   Nenhuma movimentacao encontrada para reinserir.")

        print("   Revisando clifor das entradas com base em t_itens.cdfor...")
        qtd_clifor_revisado = revisar_clifor_entradas(conn, codigo_item=codigo_item)
        print(f"   Registros com clifor revisado: {qtd_clifor_revisado}")

        print("   Criando copia de seguranca da t_saldoit antes do update final...")
        tabela_backup_saldoit = criar_copia_seguranca_t_saldoit(conn)
        print(f"   Copia de seguranca criada: {tabela_backup_saldoit}")

        print("   Atualizando t_saldoit com base no ultimo movimento de cada item/empresa...")
        qtd_saldos_atualizados = atualizar_saldos_finais(conn, codigo_item=codigo_item)
        print(f"   Registros atualizados em t_saldoit: {qtd_saldos_atualizados}")

    if codigo_item is None:
        print("7) Recriando indices, constraints, PK, FK e triggers na nova T_MOVEST...")
        caminho_script = replicar_estrutura_t_movest(engine_atual, tabela_inventario)
        print(f"Script SQL salvo em: {caminho_script}")
    else:
        print("7) Processamento por item concluido sem recriacao de objetos da tabela.")

    print("Concluido com sucesso.")


if __name__ == "__main__":
    main()
