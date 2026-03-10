from collections import defaultdict
from datetime import datetime
from pathlib import Path

from sqlalchemy import text


def _q(name):
    return "[" + str(name).replace("]", "]]") + "]"


def _table_name(name):
    return f"dbo.{_q(name)}"


def _replace_trigger_target(definition, origem, destino):
    origem_bracket = f"ON [dbo].[{origem}]"
    destino_bracket = f"ON [dbo].[{destino}]"
    origem_plain = f"ON dbo.{origem}"
    destino_plain = f"ON dbo.{destino}"
    return definition.replace(origem_bracket, destino_bracket).replace(origem_plain, destino_plain)


def _classificar_tipo_indice(type_desc):
    valor = (type_desc or "").upper().strip()
    if valor == "CLUSTERED":
        return "CLUSTERED"
    return "NONCLUSTERED"


def _tem_indice_clusterizado(conn, tabela):
    row = conn.execute(
        text(
            """
            SELECT TOP 1 1
            FROM sys.indexes
            WHERE object_id = OBJECT_ID(:table_name)
              AND type_desc = 'CLUSTERED'
              AND is_hypothetical = 0
            """
        ),
        {"table_name": f"dbo.{tabela}"},
    ).scalar()
    return bool(row)


def _salvar_scripts_replicacao(tabela_origem, tabela_destino, drop_scripts, create_scripts):
    pasta_saida = Path("scripts_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_saida = pasta_saida / f"replicar_objetos_{tabela_origem}_para_{tabela_destino}_{timestamp}.sql"

    secoes = [
        f"-- Origem: dbo.{tabela_origem}",
        f"-- Destino: dbo.{tabela_destino}",
        f"-- Gerado em: {datetime.now().isoformat()}",
        "-- Execucao manual: rode este script apos a carga da nova T_MOVEST.",
        "",
        "-- DROP DE OBJETOS NA TABELA RENOMEADA",
        "GO",
    ]

    for script in drop_scripts:
        if not script.strip():
            continue
        secoes.append(script.strip())
        secoes.append("GO")

    secoes.extend(
        [
            "",
        "-- CREATE DE INDICES, CONSTRAINTS, PK, FK E TRIGGERS NA NOVA T_MOVEST",
        "GO",
        ]
    )

    for script in create_scripts:
        if not script.strip():
            continue
        secoes.append(script.strip())
        secoes.append("GO")

    caminho_saida.write_text("\n".join(secoes) + "\n", encoding="utf-8")
    return caminho_saida


def _salvar_scripts_indices_padrao(tabela_destino, create_scripts):
    pasta_saida = Path("scripts_gerados")
    pasta_saida.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_saida = pasta_saida / f"recriar_indices_{tabela_destino}_{timestamp}.sql"

    secoes = [
        f"-- Destino: dbo.{tabela_destino}",
        f"-- Gerado em: {datetime.now().isoformat()}",
        "-- Execucao manual: rode este script apos a carga da nova T_MOVEST.",
        "",
        "-- CREATE DE INDICES PADRAO NA NOVA T_MOVEST",
        "GO",
    ]

    for script in create_scripts:
        if not script.strip():
            continue
        secoes.append(script.strip())
        secoes.append("GO")

    caminho_saida.write_text("\n".join(secoes) + "\n", encoding="utf-8")
    return caminho_saida


def recriar_indices(tabela_destino="T_MOVEST"):
    create_scripts = [
        f"CREATE NONCLUSTERED INDEX IX_T_MOVEST_Data ON {_table_name(tabela_destino)} (DataLan, nrlan)",
        f"CREATE NONCLUSTERED INDEX IX_T_MOVEST_ItemEmp ON {_table_name(tabela_destino)} (cditem, cdemp)",
    ]
    return create_scripts, _salvar_scripts_indices_padrao(tabela_destino, create_scripts)


def _tem_coluna(conn, tabela, coluna):
    return bool(
        conn.execute(
            text(
                """
                SELECT TOP 1 1
                FROM sys.columns
                WHERE object_id = OBJECT_ID(:table_name)
                  AND name = :column_name
                """
            ),
            {"table_name": f"dbo.{tabela}", "column_name": coluna},
        ).scalar()
    )


def _colunas_tabela(conn, tabela):
    rows = conn.execute(
        text(
            """
            SELECT name
            FROM sys.columns
            WHERE object_id = OBJECT_ID(:table_name)
            """
        ),
        {"table_name": f"dbo.{tabela}"},
    ).fetchall()
    return {row._mapping["name"] for row in rows}


def _metadados_colunas_tabela(conn, tabela):
    rows = conn.execute(
        text(
            """
            SELECT
                c.name,
                t.name AS type_name,
                ts.name AS type_schema_name,
                t.is_user_defined,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.collation_name,
                c.is_identity,
                c.is_computed,
                CONVERT(VARCHAR(100), ic.seed_value) AS seed_value,
                CONVERT(VARCHAR(100), ic.increment_value) AS increment_value,
                CONVERT(NVARCHAR(MAX), cc.definition) AS computed_definition,
                cc.is_persisted,
                dc.name AS default_name,
                CONVERT(NVARCHAR(MAX), dc.definition) AS default_definition
            FROM sys.columns c
            JOIN sys.types t
              ON t.user_type_id = c.user_type_id
            JOIN sys.schemas ts
              ON ts.schema_id = t.schema_id
            LEFT JOIN sys.identity_columns ic
              ON ic.object_id = c.object_id
             AND ic.column_id = c.column_id
            LEFT JOIN sys.computed_columns cc
              ON cc.object_id = c.object_id
             AND cc.column_id = c.column_id
            LEFT JOIN sys.default_constraints dc
              ON dc.parent_object_id = c.object_id
             AND dc.parent_column_id = c.column_id
            WHERE c.object_id = OBJECT_ID(:table_name)
            """
        ),
        {"table_name": f"dbo.{tabela}"},
    ).fetchall()
    return {row._mapping["name"]: dict(row._mapping) for row in rows}


def _formatar_numero_sql(valor):
    if valor is None:
        return None
    try:
        numero = int(valor)
        if float(valor) == float(numero):
            return str(numero)
    except (TypeError, ValueError):
        pass
    return str(valor)


def _definicao_tipo_coluna(meta):
    tipo = meta["type_name"]
    if meta["is_user_defined"]:
        base = f"{_q(meta['type_schema_name'])}.{_q(tipo)}"
    else:
        base = tipo

    if tipo in {"varchar", "char", "varbinary", "binary"}:
        tamanho = "MAX" if meta["max_length"] == -1 else str(meta["max_length"])
        return f"{base}({tamanho})"

    if tipo in {"nvarchar", "nchar"}:
        tamanho = "MAX" if meta["max_length"] == -1 else str(int(meta["max_length"] / 2))
        return f"{base}({tamanho})"

    if tipo in {"decimal", "numeric"}:
        return f"{base}({meta['precision']},{meta['scale']})"

    if tipo in {"datetime2", "datetimeoffset", "time"}:
        return f"{base}({meta['scale']})"

    return base


def _default_fallback_coluna(meta):
    tipo = meta["type_name"]
    if tipo in {"bit"}:
        return "((0))"
    if tipo in {"tinyint", "smallint", "int", "bigint", "decimal", "numeric", "float", "real", "money", "smallmoney"}:
        return "((0))"
    if tipo in {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}:
        return "('')"
    if tipo in {"date", "datetime", "smalldatetime", "datetime2", "datetimeoffset"}:
        return "('19000101')"
    if tipo in {"time"}:
        return "('00:00:00')"
    if tipo in {"uniqueidentifier"}:
        return "(NEWID())"
    if tipo in {"binary", "varbinary"}:
        return "(0x)"
    return None


def _montar_sql_add_coluna(meta, tabela_destino, nome_constraint_default=None, default_definition=None):
    coluna = _q(meta["name"])
    if meta["is_computed"]:
        persisted_sql = " PERSISTED" if meta["is_persisted"] else ""
        return (
            f"ALTER TABLE {_table_name(tabela_destino)} "
            f"ADD {coluna} AS {meta['computed_definition']}{persisted_sql}"
        )

    partes = [f"ALTER TABLE {_table_name(tabela_destino)} ADD {coluna} {_definicao_tipo_coluna(meta)}"]

    if meta["collation_name"] and meta["type_name"] in {"char", "varchar", "nchar", "nvarchar", "text", "ntext"}:
        partes.append(f"COLLATE {meta['collation_name']}")

    if meta["is_identity"]:
        seed = _formatar_numero_sql(meta["seed_value"]) or "1"
        inc = _formatar_numero_sql(meta["increment_value"]) or "1"
        partes.append(f"IDENTITY({seed},{inc})")

    default_sql = default_definition
    if default_sql and nome_constraint_default:
        partes.append(f"CONSTRAINT {_q(nome_constraint_default)} DEFAULT {default_sql} WITH VALUES")
    elif default_sql:
        partes.append(f"DEFAULT {default_sql} WITH VALUES")

    partes.append("NULL" if meta["is_nullable"] else "NOT NULL")
    return " ".join(partes)


def atualizar_saldos_finais(conn, codigo_item=None):
    coluna_data = "[DataLan]" if _tem_coluna(conn, "T_MOVEST", "DataLan") else "[data]"
    coluna_nrlan = "TRY_CAST([nrlan] AS BIGINT)" if _tem_coluna(conn, "T_MOVEST", "nrlan") else "0"
    if _tem_coluna(conn, "T_MOVEST", "SEQIT"):
        coluna_seqit = "TRY_CAST([SEQIT] AS BIGINT)"
    elif _tem_coluna(conn, "T_MOVEST", "Registro"):
        coluna_seqit = "TRY_CAST([Registro] AS BIGINT)"
    else:
        coluna_seqit = "0"
    coluna_numdoc = "TRY_CAST([numdoc] AS BIGINT)" if _tem_coluna(conn, "T_MOVEST", "numdoc") else "0"
    expr_empitem_movest = "[empitem]" if _tem_coluna(conn, "T_MOVEST", "empitem") else "1"
    expr_empitem_saldoit = "s.[empitem]" if _tem_coluna(conn, "t_saldoit", "empitem") else "1"

    filtro_movest = ""
    filtro_saldoit = ""
    params = {}
    if codigo_item is not None:
        filtro_movest = "WHERE m.cditem = :codigo_item"
        filtro_saldoit = "WHERE s.cditem = :codigo_item"
        params["codigo_item"] = codigo_item

    resultado = conn.execute(
        text(
            f"""
            WITH ultimos AS (
                SELECT
                    m.cditem,
                    m.cdemp,
                    {expr_empitem_movest} AS empitem,
                    CASE
                        WHEN m.st = 'E' THEN ISNULL(m.SldAntEmp, 0) + ISNULL(m.qtde, 0)
                        WHEN m.st = 'S' THEN ISNULL(m.SldAntEmp, 0) - ISNULL(m.qtde, 0)
                        ELSE ISNULL(m.SldAntEmp, 0)
                    END AS saldo_final,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.cditem, m.cdemp, {expr_empitem_movest}
                        ORDER BY {coluna_data} DESC, {coluna_nrlan} DESC, {coluna_seqit} DESC, {coluna_numdoc} DESC
                    ) AS rn
                FROM dbo.T_MOVEST m
                {filtro_movest}
            )
            UPDATE s
            SET s.saldo = u.saldo_final
            FROM dbo.t_saldoit s
            JOIN ultimos u
              ON u.rn = 1
             AND u.cditem = s.cditem
             AND u.cdemp = s.cdemp
             AND u.empitem = {expr_empitem_saldoit}
            {filtro_saldoit}
            """
        ),
        params,
    )
    return int(resultado.rowcount or 0)


def replicar_estrutura_t_movest(engine, tabela_origem, tabela_destino="T_MOVEST"):
    if not tabela_origem:
        create_scripts, caminho_scripts = recriar_indices(tabela_destino)
        with engine.begin() as conn:
            for script in create_scripts:
                conn.execute(text(script))
        return caminho_scripts

    origem_full = f"dbo.{tabela_origem}"

    with engine.begin() as conn:
        destino_tem_clustered = _tem_indice_clusterizado(conn, tabela_destino)
        colunas_destino = _colunas_tabela(conn, tabela_destino)
        metadados_colunas_origem = _metadados_colunas_tabela(conn, tabela_origem)

        default_rows = conn.execute(
            text(
                """
                SELECT dc.name, c.name AS column_name, dc.definition
                FROM sys.default_constraints dc
                JOIN sys.columns c
                  ON c.object_id = dc.parent_object_id
                 AND c.column_id = dc.parent_column_id
                WHERE dc.parent_object_id = OBJECT_ID(:table_name)
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        check_rows = conn.execute(
            text(
                """
                SELECT
                    cc.name,
                    cc.definition,
                    col.name AS column_name
                FROM sys.check_constraints cc
                LEFT JOIN sys.sql_expression_dependencies dep
                  ON dep.referencing_id = cc.object_id
                 AND dep.referenced_id = cc.parent_object_id
                LEFT JOIN sys.columns col
                  ON col.object_id = cc.parent_object_id
                 AND col.column_id = dep.referenced_minor_id
                WHERE cc.parent_object_id = OBJECT_ID(:table_name)
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        key_rows = conn.execute(
            text(
                """
                SELECT
                    kc.name,
                    kc.type_desc,
                    i.type_desc AS index_type_desc,
                    ic.key_ordinal,
                    c.name AS column_name,
                    ic.is_descending_key
                FROM sys.key_constraints kc
                JOIN sys.indexes i
                  ON i.object_id = kc.parent_object_id
                 AND i.index_id = kc.unique_index_id
                JOIN sys.index_columns ic
                  ON ic.object_id = i.object_id
                 AND ic.index_id = i.index_id
                JOIN sys.columns c
                  ON c.object_id = ic.object_id
                 AND c.column_id = ic.column_id
                WHERE kc.parent_object_id = OBJECT_ID(:table_name)
                ORDER BY kc.name, ic.key_ordinal
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        fk_rows = conn.execute(
            text(
                """
                SELECT
                    fk.name,
                    fk.delete_referential_action_desc,
                    fk.update_referential_action_desc,
                    fkc.constraint_column_id,
                    pc.name AS parent_column_name,
                    rs.name AS ref_schema_name,
                    rt.name AS ref_table_name,
                    rc.name AS ref_column_name
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                  ON fkc.constraint_object_id = fk.object_id
                JOIN sys.columns pc
                  ON pc.object_id = fkc.parent_object_id
                 AND pc.column_id = fkc.parent_column_id
                JOIN sys.tables rt
                  ON rt.object_id = fkc.referenced_object_id
                JOIN sys.schemas rs
                  ON rs.schema_id = rt.schema_id
                JOIN sys.columns rc
                  ON rc.object_id = fkc.referenced_object_id
                 AND rc.column_id = fkc.referenced_column_id
                WHERE fk.parent_object_id = OBJECT_ID(:table_name)
                ORDER BY fk.name, fkc.constraint_column_id
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        index_rows = conn.execute(
            text(
                """
                SELECT
                    i.name,
                    i.type_desc,
                    i.is_unique,
                    i.filter_definition,
                    ic.key_ordinal,
                    ic.is_included_column,
                    ic.index_column_id,
                    ic.is_descending_key,
                    c.name AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic
                  ON ic.object_id = i.object_id
                 AND ic.index_id = i.index_id
                JOIN sys.columns c
                  ON c.object_id = ic.object_id
                 AND c.column_id = ic.column_id
                WHERE i.object_id = OBJECT_ID(:table_name)
                  AND i.name IS NOT NULL
                  AND i.is_primary_key = 0
                  AND i.is_unique_constraint = 0
                  AND i.is_hypothetical = 0
                ORDER BY i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        trigger_rows = conn.execute(
            text(
                """
                SELECT tr.name, OBJECT_DEFINITION(tr.object_id) AS definition
                FROM sys.triggers tr
                WHERE tr.parent_id = OBJECT_ID(:table_name)
                """
            ),
            {"table_name": origem_full},
        ).fetchall()

        key_map = defaultdict(lambda: {"defs": [], "column_names": set()})
        key_meta = {}
        for row in key_rows:
            data = row._mapping
            key_meta[data["name"]] = (data["type_desc"], data["index_type_desc"])
            order = " DESC" if data["is_descending_key"] else " ASC"
            key_map[data["name"]]["defs"].append(f"{_q(data['column_name'])}{order}")
            key_map[data["name"]]["column_names"].add(data["column_name"])

        fk_map = defaultdict(list)
        fk_meta = {}
        for row in fk_rows:
            data = row._mapping
            fk_meta[data["name"]] = (
                data["delete_referential_action_desc"],
                data["update_referential_action_desc"],
                data["ref_schema_name"],
                data["ref_table_name"],
            )
            fk_map[data["name"]].append((data["parent_column_name"], data["ref_column_name"]))

        check_map = defaultdict(lambda: {"definition": None, "column_names": set()})
        for row in check_rows:
            data = row._mapping
            check_map[data["name"]]["definition"] = data["definition"]
            if data["column_name"]:
                check_map[data["name"]]["column_names"].add(data["column_name"])

        index_map = defaultdict(lambda: {"keys": [], "includes": [], "meta": None, "column_names": set()})
        for row in index_rows:
            data = row._mapping
            index_map[data["name"]]["meta"] = (
                data["type_desc"],
                data["is_unique"],
                data["filter_definition"],
            )
            index_map[data["name"]]["column_names"].add(data["column_name"])
            if data["is_included_column"]:
                index_map[data["name"]]["includes"].append(_q(data["column_name"]))
            else:
                order = " DESC" if data["is_descending_key"] else " ASC"
                index_map[data["name"]]["keys"].append(f"{_q(data['column_name'])}{order}")

        drop_scripts = []
        create_scripts = []
        default_constraints_inline = set()

        colunas_importantes = set()
        for item in key_map.values():
            colunas_importantes.update(item["column_names"])
        for item in check_map.values():
            colunas_importantes.update(item["column_names"])
        for row in default_rows:
            colunas_importantes.add(row._mapping["column_name"])
        for item in index_map.values():
            colunas_importantes.update(item["column_names"])
        for pairs in fk_map.values():
            colunas_importantes.update(parent for parent, _ in pairs)

        colunas_faltantes = [col for col in sorted(colunas_importantes) if col not in colunas_destino]
        for nome_coluna in colunas_faltantes:
            meta = metadados_colunas_origem.get(nome_coluna)
            if not meta:
                continue
            meta = dict(meta)

            default_nome = meta.get("default_name")
            default_def = meta.get("default_definition")
            if not meta["is_nullable"] and not default_def and not meta["is_identity"] and not meta["is_computed"]:
                default_def = _default_fallback_coluna(meta)
                if default_def:
                    default_nome = f"DF_{tabela_destino}_{nome_coluna}_AUTO"
                else:
                    meta["is_nullable"] = True

            create_scripts.append(
                _montar_sql_add_coluna(
                    meta,
                    tabela_destino,
                    nome_constraint_default=default_nome if default_def else None,
                    default_definition=default_def,
                )
            )
            colunas_destino.add(nome_coluna)
            if meta.get("default_name") and default_nome == meta.get("default_name") and default_def:
                default_constraints_inline.add(meta["default_name"])

        for name, item in key_map.items():
            if not item["column_names"].issubset(colunas_destino):
                continue
            key_type, index_type = key_meta[name]
            constraint_type = "PRIMARY KEY" if key_type == "PK_CONSTRAINT" else "UNIQUE"
            clustered = _classificar_tipo_indice(index_type)
            if clustered == "CLUSTERED" and destino_tem_clustered:
                clustered = "NONCLUSTERED"
            elif clustered == "CLUSTERED":
                destino_tem_clustered = True

            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(name)}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(name)} {constraint_type} {clustered} "
                f"({', '.join(item['defs'])})"
            )

        for name, item in check_map.items():
            if item["column_names"] and not item["column_names"].issubset(colunas_destino):
                continue
            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(name)}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(name)} CHECK {item['definition']}"
            )

        for row in default_rows:
            data = row._mapping
            if data["name"] in default_constraints_inline:
                continue
            if data["column_name"] not in colunas_destino:
                continue
            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(data['name'])}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(data['name'])} "
                f"DEFAULT {data['definition']} FOR {_q(data['column_name'])}"
            )

        for name, item in index_map.items():
            if not item["column_names"].issubset(colunas_destino):
                continue
            type_desc, is_unique, filter_definition = item["meta"]
            unique_sql = "UNIQUE " if is_unique else ""
            index_type = _classificar_tipo_indice(type_desc)
            if index_type == "CLUSTERED" and destino_tem_clustered:
                index_type = "NONCLUSTERED"
            elif index_type == "CLUSTERED":
                destino_tem_clustered = True
            include_sql = f" INCLUDE ({', '.join(item['includes'])})" if item["includes"] else ""
            filter_sql = f" WHERE {filter_definition}" if filter_definition else ""

            drop_scripts.append(f"DROP INDEX {_q(name)} ON {_table_name(tabela_origem)}")
            create_scripts.append(
                f"CREATE {unique_sql}{index_type} INDEX {_q(name)} "
                f"ON {_table_name(tabela_destino)} ({', '.join(item['keys'])}){include_sql}{filter_sql}"
            )

        for name, pairs in fk_map.items():
            colunas_fk = {parent for parent, _ in pairs}
            if not colunas_fk.issubset(colunas_destino):
                continue
            delete_action, update_action, ref_schema, ref_table = fk_meta[name]
            parent_cols = ", ".join(_q(parent) for parent, _ in pairs)
            ref_cols = ", ".join(_q(ref) for _, ref in pairs)
            delete_sql = "" if delete_action == "NO_ACTION" else f" ON DELETE {delete_action.replace('_', ' ')}"
            update_sql = "" if update_action == "NO_ACTION" else f" ON UPDATE {update_action.replace('_', ' ')}"

            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(name)}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(name)} FOREIGN KEY ({parent_cols}) "
                f"REFERENCES {_q(ref_schema)}.{_q(ref_table)} ({ref_cols}){delete_sql}{update_sql}"
            )

        for row in trigger_rows:
            data = row._mapping
            drop_scripts.append(f"DROP TRIGGER {_table_name(data['name'])}")
            create_scripts.append(_replace_trigger_target(data["definition"], tabela_origem, tabela_destino))

        caminho_scripts = _salvar_scripts_replicacao(
            tabela_origem,
            tabela_destino,
            drop_scripts,
            create_scripts,
        )

        for script in drop_scripts:
            conn.execute(text(script))

        for script in create_scripts:
            conn.execute(text(script))

        return caminho_scripts
