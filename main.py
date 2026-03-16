import os
import subprocess
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*args, **kwargs):
        return False

from src.auditoria import auditar_saldos_pos_update
from src.database import get_engine
from src.transform import extrair_movimentacoes_novas
from src.utils import (
    atualizar_saldos_discrepantes,
    atualizar_saldos_finais,
    criar_copia_seguranca_t_saldoit,
    limpar_residuos_movest,
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
            padrao=True,
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
            montar_mensagem_erro_conexao(exc, nome_banco, papel, servidor)
        ) from exc


def listar_instancias_sql_local():
    if os.name != "nt":
        return []

    try:
        resultado = subprocess.run(
            ["sc", "query", "type=", "service", "state=", "all"],
            capture_output=True,
            text=True,
            encoding="cp1252",
            errors="ignore",
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return []

    instancias = []
    for linha in resultado.stdout.splitlines():
        linha = linha.strip()
        if not linha.startswith("SERVICE_NAME:"):
            continue

        nome_servico = linha.split(":", 1)[1].strip()
        if nome_servico == "MSSQLSERVER":
            instancias.append("localhost")
        elif nome_servico.startswith("MSSQL$"):
            instancias.append(f"localhost\\{nome_servico.split('$', 1)[1]}")

    return sorted(set(instancias))


def montar_mensagem_erro_conexao(exc, nome_banco, papel, servidor):
    mensagem = (
        f"Nao foi possivel conectar ao banco {papel} '{nome_banco}' no servidor '{servidor}'.\n"
        "Verifique se o SQL Server esta ligado, se o nome da instancia esta correto, "
        "se o banco existe e se o usuario informado tem permissao.\n"
        "Exemplos de servidor validos: localhost, localhost\\SQLEXPRESS, SERVIDOR\\INSTANCIA, 127.0.0.1,1433."
    )

    servidor_normalizado = servidor.strip().lower()
    if servidor_normalizado in {"localhost", ".", "(local)", "127.0.0.1"}:
        instancias = listar_instancias_sql_local()
        if instancias:
            mensagem += "\nInstancias locais detectadas nesta maquina: " + ", ".join(instancias) + "."
        else:
            mensagem += (
                "\nNenhuma instancia local de SQL Server foi detectada automaticamente. "
                "Se voce usa uma instancia nomeada, informe no formato localhost\\NOME_DA_INSTANCIA."
            )

    detalhes = str(exc).strip()
    if detalhes:
        mensagem += f"\nDetalhe tecnico: {detalhes}"

    return mensagem


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
        if conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'data')")).scalar():
            coluna_data_base = "[data]"
        elif conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'DataLan')")).scalar():
            coluna_data_base = "DataLan"
        elif conn.execute(text("SELECT COL_LENGTH('dbo.T_MOVEST', 'datadoc')")).scalar():
            coluna_data_base = "datadoc"
        else:
            raise RuntimeError("A dbo.T_MOVEST nao possui data, DATADOC ou DataLan para definir a data base.")

        data_maxima_original = conn.execute(
            text(f"SELECT MAX({coluna_data_base}) FROM T_MOVEST")
        ).scalar()
        data_limite = datetime.now()
        data_candidata = data_maxima_original

        while data_candidata and data_candidata > data_limite:
            data_candidata = conn.execute(
                text(
                    f"""
                    SELECT MAX({coluna_data_base})
                    FROM T_MOVEST
                    WHERE {coluna_data_base} < :data_anterior
                    """
                ),
                {"data_anterior": data_candidata},
            ).scalar()

    return (data_candidata or datetime(1900, 1, 1)), data_maxima_original, coluna_data_base


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


def revisar_clifor_vendas(conn, data_corte, codigo_item=None):
    requisitos = [
        ("T_MOVEST", "clifor"),
        ("T_MOVEST", "especie"),
        ("T_MOVEST", "numdoc"),
        ("T_MOVEST", "cditem"),
        ("T_VENDAS", "nrven_v"),
        ("T_VENDAS", "cdcli_v"),
        ("T_VENDAS", "emisven_v"),
    ]
    for tabela, coluna in requisitos:
        if not conn.execute(text("SELECT COL_LENGTH(:tabela, :coluna)"), {"tabela": f"dbo.{tabela}", "coluna": coluna}).scalar():
            return 0

    filtro_item = ""
    params = {"data_corte": data_corte}
    if codigo_item is not None:
        filtro_item = " AND m.cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    resultado = conn.execute(
        text(
            f"""
            UPDATE m
            SET clifor = v.cdcli_v
            FROM dbo.T_MOVEST m
            INNER JOIN dbo.T_VENDAS v
                ON m.numdoc = v.nrven_v
            WHERE m.especie = 'V'
              AND m.clifor <> v.cdcli_v
              AND v.emisven_v >= :data_corte
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
            SELECT
                c.name AS column_name,
                TYPE_NAME(c.user_type_id) AS type_name,
                c.max_length
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('dbo.T_MOVEST')
            """
        )
    ).fetchall()
    mapa_tipos = {
        r._mapping["column_name"]: {
            "type_name": r._mapping["type_name"].lower(),
            "max_length": r._mapping["max_length"],
        }
        for r in tipos
    }

    truncamentos = []

    tipos_int = {"int", "bigint", "smallint", "tinyint"}
    tipos_texto = {"char", "varchar", "nchar", "nvarchar"}
    for col in list(df_para_gravar.columns):
        meta = mapa_tipos.get(col)
        if not meta:
            continue

        tipo = meta["type_name"]
        if tipo in tipos_int:
            serie = df_para_gravar[col].astype("string").str.strip()
            serie = serie.str.replace(r"[^0-9-]", "", regex=True)
            serie = serie.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})
            nums = pd.to_numeric(serie, errors="coerce")
            df_para_gravar[col] = nums.apply(lambda x: int(x) if pd.notna(x) else None)

        if tipo == "bit":
            nums = pd.to_numeric(df_para_gravar[col], errors="coerce").fillna(0)
            df_para_gravar[col] = nums.apply(lambda x: 1 if int(x) != 0 else 0)

        if tipo in tipos_texto and meta["max_length"] not in (None, -1):
            limite = int(meta["max_length"])
            if tipo in {"nchar", "nvarchar"}:
                limite = int(limite / 2)
            if limite <= 0:
                continue

            serie = df_para_gravar[col]
            mascara_valida = serie.notna()
            if not mascara_valida.any():
                continue

            serie_texto = serie.astype("string")
            comprimentos = serie_texto.str.len()
            mascara_truncar = mascara_valida & comprimentos.gt(limite)
            if mascara_truncar.any():
                truncamentos.append(
                    {
                        "coluna": col,
                        "limite": limite,
                        "qtd_registros": int(mascara_truncar.sum()),
                        "maior_tamanho_original": int(comprimentos[mascara_truncar].max()),
                    }
                )
                df_para_gravar.loc[mascara_truncar, col] = serie_texto.loc[mascara_truncar].str.slice(
                    0, limite
                )

    return df_para_gravar, truncamentos


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


def valor_numerico(value):
    numero = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numero):
        return None
    return float(numero)


def delta_compativel_com_st(delta, st, tolerancia=0.000001):
    st_upper = str(st).upper().strip()
    if st_upper == "E":
        return delta >= -tolerancia
    if st_upper == "S":
        return delta <= tolerancia
    return False


def recalcular_qtde_movimento_preservando_resultado(
    row,
    saldo_item_atual,
    saldo_item_emp_atual,
    tolerancia=0.000001,
):
    st_original = str(row.get("st", "")).upper().strip()
    qtde_original = valor_numerico(row.get("qtde")) or 0

    alvo_final_geral = valor_numerico(row.get("_resultado_intencao_geral"))
    alvo_final_emp = valor_numerico(row.get("_resultado_intencao_emp"))

    if (
        st_original not in {"E", "S"}
        or (alvo_final_geral is None and alvo_final_emp is None)
    ):
        return qtde_original, st_original, None

    delta_geral = None if alvo_final_geral is None else float(alvo_final_geral) - float(saldo_item_atual)
    delta_emp = None if alvo_final_emp is None else float(alvo_final_emp) - float(saldo_item_emp_atual)

    divergencia_alvos = (
        delta_geral is not None
        and delta_emp is not None
        and abs(delta_geral - delta_emp) > tolerancia
    )

    delta_escolhido = None
    origem_alvo = None

    if delta_emp is not None and delta_compativel_com_st(
        delta_emp, st_original, tolerancia=tolerancia
    ):
        delta_escolhido = delta_emp
        origem_alvo = "sldantemp"
    elif delta_geral is not None and delta_compativel_com_st(
        delta_geral, st_original, tolerancia=tolerancia
    ):
        delta_escolhido = delta_geral
        origem_alvo = "saldoant"
    elif delta_emp is not None:
        delta_escolhido = delta_emp
        origem_alvo = "sldantemp"
    elif delta_geral is not None:
        delta_escolhido = delta_geral
        origem_alvo = "saldoant"

    if delta_escolhido is None:
        return qtde_original, st_original, {
            "status": "incompativel",
            "qtde_original": qtde_original,
            "alvo_final_geral": alvo_final_geral,
            "alvo_final_emp": alvo_final_emp,
            "delta_geral": delta_geral,
            "delta_emp": delta_emp,
            "divergencia_alvos": divergencia_alvos,
        }

    if abs(delta_escolhido) <= tolerancia:
        novo_st = st_original
        nova_qtde = 0.0
    else:
        novo_st = "E" if delta_escolhido > 0 else "S"
        nova_qtde = abs(float(delta_escolhido))

    st_alterado = novo_st != st_original
    qtde_alterada = abs(nova_qtde - qtde_original) > tolerancia
    if st_alterado and qtde_alterada:
        status = "recalculado_com_troca_st"
    elif st_alterado:
        status = "troca_st"
    elif qtde_alterada:
        status = "recalculado"
    else:
        status = "inalterado"

    return nova_qtde, novo_st, {
        "status": status,
        "qtde_original": qtde_original,
        "qtde_nova": nova_qtde,
        "st_original": st_original,
        "st_novo": novo_st,
        "alvo_final_geral": alvo_final_geral,
        "alvo_final_emp": alvo_final_emp,
        "delta_geral": delta_geral,
        "delta_emp": delta_emp,
        "origem_alvo": origem_alvo,
        "divergencia_alvos": divergencia_alvos,
    }


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
    qtde_recalculada = []
    st_recalculado = []
    qtd_avulsas_identificadas = 0
    qtd_avulsas_recalculadas = 0
    qtd_avulsas_inalteradas = 0
    qtd_avulsas_incompativeis = 0
    qtd_avulsas_divergentes = 0
    qtd_avulsas_st_corrigido = 0
    qtd_inventarios_identificados = 0
    qtd_inventarios_recalculados = 0
    qtd_inventarios_inalterados = 0
    qtd_inventarios_incompativeis = 0
    qtd_inventarios_divergentes = 0
    qtd_inventarios_st_corrigido = 0
    exemplos_incompativeis = []
    exemplos_avulsos_incompativeis = []

    for _, row in df_novos.iterrows():
        cditem, cdemp = row["cditem"], row["cdemp"]
        saldo_item = dict_geral.get(cditem, 0)
        saldo_item_emp = dict_emp.get((cditem, cdemp), 0)

        qtde_mov = row["qtde"]
        st_mov = row["st"]
        especie_mov = str(row.get("especie", "")).upper().strip()
        info_recalculo = None
        if especie_mov == "I":
            qtd_inventarios_identificados += 1
            qtde_mov, st_mov, info_recalculo = recalcular_qtde_movimento_preservando_resultado(
                row,
                saldo_item,
                saldo_item_emp,
            )
            if info_recalculo:
                if info_recalculo.get("divergencia_alvos"):
                    qtd_inventarios_divergentes += 1
                if info_recalculo.get("status") in {"recalculado", "recalculado_com_troca_st"}:
                    qtd_inventarios_recalculados += 1
                if info_recalculo.get("status") in {"troca_st", "recalculado_com_troca_st"}:
                    qtd_inventarios_st_corrigido += 1
                elif info_recalculo.get("status") == "inalterado":
                    qtd_inventarios_inalterados += 1
                elif info_recalculo.get("status") == "incompativel":
                    qtd_inventarios_incompativeis += 1
                    if len(exemplos_incompativeis) < 10:
                        exemplos_incompativeis.append(
                            {
                                "data": row.get("DataLan", row.get("data")),
                                "numdoc": row.get("numdoc"),
                                "cdemp": cdemp,
                                "cditem": cditem,
                                "st": row.get("st"),
                                "qtde_original": info_recalculo.get("qtde_original"),
                                "delta_geral": info_recalculo.get("delta_geral"),
                                "delta_emp": info_recalculo.get("delta_emp"),
                            }
                        )
        elif (
            valor_numerico(row.get("_resultado_intencao_geral")) is not None
            or valor_numerico(row.get("_resultado_intencao_emp")) is not None
        ):
            qtd_avulsas_identificadas += 1
            qtde_mov, st_mov, info_recalculo = recalcular_qtde_movimento_preservando_resultado(
                row,
                saldo_item,
                saldo_item_emp,
            )
            if info_recalculo:
                if info_recalculo.get("divergencia_alvos"):
                    qtd_avulsas_divergentes += 1
                if info_recalculo.get("status") in {"recalculado", "recalculado_com_troca_st"}:
                    qtd_avulsas_recalculadas += 1
                if info_recalculo.get("status") in {"troca_st", "recalculado_com_troca_st"}:
                    qtd_avulsas_st_corrigido += 1
                elif info_recalculo.get("status") == "inalterado":
                    qtd_avulsas_inalteradas += 1
                elif info_recalculo.get("status") == "incompativel":
                    qtd_avulsas_incompativeis += 1
                    if len(exemplos_avulsos_incompativeis) < 10:
                        exemplos_avulsos_incompativeis.append(
                            {
                                "data": row.get("DataLan", row.get("data")),
                                "numdoc": row.get("numdoc"),
                                "cdemp": cdemp,
                                "cditem": cditem,
                                "especie": especie_mov,
                                "st": row.get("st"),
                                "qtde_original": info_recalculo.get("qtde_original"),
                                "delta_geral": info_recalculo.get("delta_geral"),
                                "delta_emp": info_recalculo.get("delta_emp"),
                            }
                        )

        saldo_ant_geral.append(saldo_item)
        saldo_ant_emp.append(saldo_item_emp)
        qtde_recalculada.append(qtde_mov)
        st_recalculado.append(st_mov)

        delta = calcular_delta(qtde_mov, st_mov)

        dict_geral[cditem] = saldo_item + delta
        dict_emp[(cditem, cdemp)] = saldo_item_emp + delta

    df_novos["qtde"] = qtde_recalculada
    df_novos["st"] = st_recalculado
    df_novos["saldoant"] = saldo_ant_geral
    df_novos["SldAntEmp"] = saldo_ant_emp
    itens_reconstruidos = []
    if not df_novos.empty:
        itens_reconstruidos = sorted(
            {int(cditem) for cditem in df_novos["cditem"].dropna().tolist()}
        )
    elif codigo_item is not None:
        itens_reconstruidos = [int(codigo_item)]
    if qtd_inventarios_identificados:
        print(
            "   Inventarios identificados: "
            f"{qtd_inventarios_identificados} | "
            f"recalculados: {qtd_inventarios_recalculados} | "
            f"st corrigido: {qtd_inventarios_st_corrigido} | "
            f"inalterados: {qtd_inventarios_inalterados} | "
            f"incompativeis: {qtd_inventarios_incompativeis} | "
            f"divergencia entre saldoant e sldantemp: {qtd_inventarios_divergentes}"
        )
        if exemplos_incompativeis:
            print("   Exemplos de inventarios incompativeis com o sentido original do ajuste:")
            for exemplo in exemplos_incompativeis:
                print(
                    "   "
                    f"data={exemplo['data']} numdoc={exemplo['numdoc']} cdemp={exemplo['cdemp']} "
                    f"cditem={exemplo['cditem']} st={exemplo['st']} "
                    f"qtde_original={exemplo['qtde_original']} "
                    f"delta_geral={exemplo['delta_geral']} delta_emp={exemplo['delta_emp']}"
                )
    if qtd_avulsas_identificadas:
        print(
            "   Avulsas com resultado intencional identificado: "
            f"{qtd_avulsas_identificadas} | "
            f"recalculadas: {qtd_avulsas_recalculadas} | "
            f"st corrigido: {qtd_avulsas_st_corrigido} | "
            f"inalteradas: {qtd_avulsas_inalteradas} | "
            f"incompativeis: {qtd_avulsas_incompativeis} | "
            f"divergencia entre saldoant e sldantemp: {qtd_avulsas_divergentes}"
        )
        if exemplos_avulsos_incompativeis:
            print("   Exemplos de avulsas incompativeis com o sentido original do ajuste:")
            for exemplo in exemplos_avulsos_incompativeis:
                print(
                    "   "
                    f"data={exemplo['data']} numdoc={exemplo['numdoc']} cdemp={exemplo['cdemp']} "
                    f"cditem={exemplo['cditem']} especie={exemplo['especie']} st={exemplo['st']} "
                    f"qtde_original={exemplo['qtde_original']} "
                    f"delta_geral={exemplo['delta_geral']} delta_emp={exemplo['delta_emp']}"
                )

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
            df_para_gravar, truncamentos_texto = normalizar_tipos_para_insert(df_para_gravar, conn)
            validar_datalan_igual_data(df_para_gravar)
            if truncamentos_texto:
                print("   Ajustando textos para caber no schema da T_MOVEST...")
                for item in truncamentos_texto:
                    print(
                        "   "
                        f"Coluna {item['coluna']}: {item['qtd_registros']} registro(s) truncado(s) "
                        f"para {item['limite']} caractere(s); maior valor original com "
                        f"{item['maior_tamanho_original']} caractere(s)."
                    )
            print(f"   Inserindo {len(df_para_gravar)} registros em T_MOVEST...")
            df_para_gravar.to_sql("T_MOVEST", conn, if_exists="append", index=False, chunksize=1000)
            print("   Insercao em T_MOVEST concluida.")
        else:
            print("   Nenhuma movimentacao encontrada para reinserir.")

        print("   Limpando residuos da T_MOVEST sem item ou empresa validos...")
        limpeza_movest = limpar_residuos_movest(conn, codigo_item=codigo_item)
        print(
            "   "
            f"Registros excluidos: {limpeza_movest['qtd_excluidos']} | "
            f"cditem sem cadastro em t_itens: {limpeza_movest['qtd_cditem_invalido']} | "
            f"cdemp sem cadastro em t_emp: {limpeza_movest['qtd_cdemp_invalido']}"
        )

        print("   Revisando clifor das entradas com base em t_itens.cdfor...")
        qtd_clifor_revisado = revisar_clifor_entradas(conn, codigo_item=codigo_item)
        print(f"   Registros com clifor revisado: {qtd_clifor_revisado}")

        print("   Criando copia de seguranca da t_saldoit antes do update final...")
        tabela_backup_saldoit = criar_copia_seguranca_t_saldoit(conn)
        print(f"   Copia de seguranca criada: {tabela_backup_saldoit}")

        print("   Atualizando t_saldoit com base no ultimo movimento de cada item/empresa...")
        qtd_saldos_atualizados = atualizar_saldos_finais(
            conn,
            codigo_item=codigo_item,
            itens=itens_reconstruidos if codigo_item is not None else None,
        )
        print(f"   Registros atualizados em t_saldoit: {qtd_saldos_atualizados}")

    if codigo_item is None:
        print("7) Recriando indices, constraints, PK, FK e triggers na nova T_MOVEST...")
        caminho_script = replicar_estrutura_t_movest(engine_atual, tabela_inventario)
        print(f"Script SQL salvo em: {caminho_script}")
    else:
        print("7) Processamento por item concluido sem recriacao de objetos da tabela.")

    print("8) Validando discrepancias entre t_saldoit e o ultimo registro da T_MOVEST...")
    df_discrepancias_saldo, resumo_saldo = auditar_saldos_pos_update(
        engine_atual,
        data_corte,
        codigo_item=None,
        codigo_empresa=None,
    )
    qtd_itens_discrepantes = 0
    if not df_discrepancias_saldo.empty:
        qtd_itens_discrepantes = int(df_discrepancias_saldo["cditem"].dropna().nunique())
    print(
        "   "
        f"Itens discrepantes: {qtd_itens_discrepantes} | "
        f"pares cditem/cdemp/empitem discrepantes: {resumo_saldo['qtd_discrepancias']}"
    )

    if not df_discrepancias_saldo.empty:
        deseja_atualizar_discrepantes = solicitar_confirmacao(
            "Deseja atualizar a t_saldoit para os itens discrepantes identificados?",
            padrao=False,
        )
        if deseja_atualizar_discrepantes:
            with engine_atual.begin() as conn:
                qtd_corrigidos = atualizar_saldos_discrepantes(conn, df_discrepancias_saldo)
            print(f"   Registros ajustados na t_saldoit a partir das discrepancias: {qtd_corrigidos}")

            df_discrepancias_saldo, resumo_saldo = auditar_saldos_pos_update(
                engine_atual,
                data_corte,
                codigo_item=None,
                codigo_empresa=None,
            )
            print(
                "   "
                f"Discrepancias restantes apos ajuste opcional: {resumo_saldo['qtd_discrepancias']}"
            )

    print("9) Aplicando ajuste final de clifor para vendas em T_MOVEST...")
    with engine_atual.begin() as conn:
        qtd_clifor_vendas = revisar_clifor_vendas(conn, data_corte, codigo_item=codigo_item)
    print(f"   Registros com clifor ajustado a partir da t_vendas: {qtd_clifor_vendas}")

    print("Concluido com sucesso.")


if __name__ == "__main__":
    main()
