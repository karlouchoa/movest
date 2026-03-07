import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text

from src.database import get_engine

DEFAULT_TABLES = [
    "T_MOVEST",
    "T_SALDOIT",
    "T_MOVEST_BKP",
    "T_VENDAS",
    "T_ITSVEN",
    "T_PDC",
    "T_ITPDC",
    "T_TRANSF",
    "T_ITTRANSF",
]

DB_ALIASES = {
    "atual": "Bancoatual",
    "base": "Bancobase",
}


def _normalize_tables(raw_tables):
    if not raw_tables:
        return DEFAULT_TABLES

    normalized = []
    seen = set()
    for value in raw_tables:
        for token in value.split(","):
            table = token.strip()
            if not table:
                continue

            lower_name = table.lower()
            if lower_name in seen:
                continue

            seen.add(lower_name)
            normalized.append(table)
    return normalized


def _resolve_db_name(db_value):
    return DB_ALIASES.get(db_value.lower(), db_value)


def _serialize_columns(columns):
    serialized = []
    for col in columns:
        serialized.append(
            {
                "name": col.get("name"),
                "type": str(col.get("type")),
                "nullable": col.get("nullable"),
                "default": col.get("default"),
                "autoincrement": col.get("autoincrement"),
            }
        )
    return serialized


def _safe_or_default(func, default_value):
    try:
        return func()
    except NotImplementedError:
        return default_value


def pull_schema(engine, schema_name, tables):
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema_name)
    table_lookup = {table_name.lower(): table_name for table_name in existing_tables}

    exported_tables = {}
    missing_tables = []

    for table in tables:
        resolved_name = table_lookup.get(table.lower())
        if not resolved_name:
            missing_tables.append(table)
            continue

        table_data = {
            "columns": _serialize_columns(
                inspector.get_columns(resolved_name, schema=schema_name)
            ),
            "primary_key": _safe_or_default(
                lambda: inspector.get_pk_constraint(resolved_name, schema=schema_name),
                {},
            ),
            "foreign_keys": _safe_or_default(
                lambda: inspector.get_foreign_keys(resolved_name, schema=schema_name),
                [],
            ),
            "unique_constraints": _safe_or_default(
                lambda: inspector.get_unique_constraints(resolved_name, schema=schema_name),
                [],
            ),
            "indexes": _safe_or_default(
                lambda: inspector.get_indexes(resolved_name, schema=schema_name),
                [],
            ),
        }
        table_data["check_constraints"] = _safe_or_default(
            lambda: inspector.get_check_constraints(resolved_name, schema=schema_name),
            [],
        )

        exported_tables[resolved_name] = table_data

    payload = {
        "database": engine.url.database,
        "schema": schema_name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tables": exported_tables,
    }
    return payload, missing_tables


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract table schema from SQL Server (simple db pull)."
    )
    parser.add_argument(
        "--db",
        default="atual",
        help="Alias 'atual'/'base' or real database name. Default: atual",
    )
    parser.add_argument(
        "--schema",
        default="dbo",
        help="SQL Server schema. Default: dbo",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Table names separated by spaces and/or commas. Default: main tables.",
    )
    parser.add_argument(
        "--output",
        help="Output JSON file. Default: schemas/<database>_<schema>_schema.json",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_name = _resolve_db_name(args.db)
    tables = _normalize_tables(args.tables)

    output_path = Path(args.output) if args.output else None
    if output_path is None:
        output_path = Path("schemas") / f"{db_name}_{args.schema}_schema.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    engine = get_engine(db_name)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    payload, missing = pull_schema(engine, args.schema, tables)

    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)

    print(f"Schema exported to: {output_path}")
    print(f"Tables exported: {len(payload['tables'])}")
    if missing:
        print("Tables not found: " + ", ".join(missing))


if __name__ == "__main__":
    main()
