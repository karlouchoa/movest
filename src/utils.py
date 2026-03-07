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
        "",
        "-- DROP NA TABELA RENOMEADA",
        "GO",
    ]

    for script in drop_scripts:
        secoes.append(script.strip())
        secoes.append("GO")

    secoes.append("")
    secoes.append("-- CREATE NA NOVA T_MOVEST")
    secoes.append("GO")

    for script in create_scripts:
        secoes.append(script.strip())
        secoes.append("GO")

    caminho_saida.write_text("\n".join(secoes) + "\n", encoding="utf-8")
    return caminho_saida


def recriar_indices(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                IF EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.T_MOVEST')
                      AND name = 'IX_T_MOVEST_Data'
                )
                DROP INDEX IX_T_MOVEST_Data ON dbo.T_MOVEST
                """
            )
        )
        conn.execute(
            text(
                """
                IF EXISTS (
                    SELECT 1
                    FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.T_MOVEST')
                      AND name = 'IX_T_MOVEST_ItemEmp'
                )
                DROP INDEX IX_T_MOVEST_ItemEmp ON dbo.T_MOVEST
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE NONCLUSTERED INDEX IX_T_MOVEST_Data
                ON dbo.T_MOVEST (DataLan, nrlan)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE NONCLUSTERED INDEX IX_T_MOVEST_ItemEmp
                ON dbo.T_MOVEST (cditem, cdemp)
                """
            )
        )


def atualizar_saldos_finais(conn, saldos_finais_item_emp):
    for (cditem, cdemp), saldo in saldos_finais_item_emp.items():
        conn.execute(
            text("UPDATE t_saldoit SET saldo = :s WHERE cditem = :i AND cdemp = :e"),
            {"s": saldo, "i": cditem, "e": cdemp},
        )


def replicar_estrutura_t_movest(engine, tabela_origem, tabela_destino="T_MOVEST"):
    if not tabela_origem:
        recriar_indices(engine)
        return

    origem_full = f"dbo.{tabela_origem}"

    with engine.begin() as conn:
        destino_tem_clustered = _tem_indice_clusterizado(conn, tabela_destino)

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
                SELECT name, definition
                FROM sys.check_constraints
                WHERE parent_object_id = OBJECT_ID(:table_name)
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

        key_map = defaultdict(list)
        key_meta = {}
        for row in key_rows:
            data = row._mapping
            key_meta[data["name"]] = (data["type_desc"], data["index_type_desc"])
            order = " DESC" if data["is_descending_key"] else " ASC"
            key_map[data["name"]].append(f"{_q(data['column_name'])}{order}")

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

        index_map = defaultdict(lambda: {"keys": [], "includes": [], "meta": None})
        for row in index_rows:
            data = row._mapping
            index_map[data["name"]]["meta"] = (
                data["type_desc"],
                data["is_unique"],
                data["filter_definition"],
            )
            if data["is_included_column"]:
                index_map[data["name"]]["includes"].append(_q(data["column_name"]))
            else:
                order = " DESC" if data["is_descending_key"] else " ASC"
                index_map[data["name"]]["keys"].append(f"{_q(data['column_name'])}{order}")

        drop_scripts = []
        create_scripts = []

        for row in trigger_rows:
            data = row._mapping
            drop_scripts.append(f"DROP TRIGGER {_table_name(data['name'])}")
            create_scripts.append(_replace_trigger_target(data["definition"], tabela_origem, tabela_destino))

        for name, pairs in fk_map.items():
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

        for name, cols in key_map.items():
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
                f"({', '.join(cols)})"
            )

        for row in check_rows:
            data = row._mapping
            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(data['name'])}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(data['name'])} CHECK {data['definition']}"
            )

        for row in default_rows:
            data = row._mapping
            drop_scripts.append(f"ALTER TABLE {_table_name(tabela_origem)} DROP CONSTRAINT {_q(data['name'])}")
            create_scripts.append(
                "ALTER TABLE "
                f"{_table_name(tabela_destino)} ADD CONSTRAINT {_q(data['name'])} "
                f"DEFAULT {data['definition']} FOR {_q(data['column_name'])}"
            )

        for name, item in index_map.items():
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

        caminho_scripts = _salvar_scripts_replicacao(
            tabela_origem,
            tabela_destino,
            drop_scripts,
            create_scripts,
        )
        print(f"Scripts salvos em: {caminho_scripts}")

        for script in drop_scripts:
            conn.execute(text(script))

        for script in create_scripts:
            conn.execute(text(script))
