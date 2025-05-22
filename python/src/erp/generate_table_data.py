#!/usr/bin/env python3
import json
import psycopg2
from psycopg2 import sql
import argparse
import sys


def infer_pg_type(value):
    """
    Infer a PostgreSQL column type from a Python value.
    """
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        # you might choose NUMERIC(10,2) instead if you want fixed precision:
        return "NUMERIC"
    else:
        return "TEXT"


def recreate_table_from_json(conn, table_name, rows):
    """
    Drop and recreate `table_name` based on keys/types in rows,
    appending the baseline columns.
    """
    if not rows:
        print(f"No data to load for table '{table_name}'. Exiting.", file=sys.stderr)
        return

    # infer columns from the first row
    first = rows[0]
    inferred = [(col, infer_pg_type(val)) for col, val in first.items()]

    # Check if all rows have the same keys
    print("Checking if all rows have the same keys...")
    with conn.cursor() as cur:
        # 1) Drop existing table
        print(f"Dropping table '{table_name}' if it exists...")
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
        )

        # 2) Build CREATE TABLE statement
        cols = [
            sql.SQL("id BIGSERIAL PRIMARY KEY"),
        ]
        for name, pgtype in inferred:
            cols.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pgtype)))

        admin_cols = [
            sql.SQL(
                "created_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP"
            ),
            sql.SQL(
                "updated_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP"
            ),
            sql.SQL("created_by TEXT DEFAULT CURRENT_USER"),
            sql.SQL("updated_by TEXT DEFAULT CURRENT_USER"),
        ]

        for col in admin_cols:
            cols.append(col)

        print(
            f"Creating table '{table_name}' with columns: {', '.join([col.as_string(conn) for col in cols])}"
        )
        create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name), sql.SQL(", ").join(cols)
        )
        cur.execute(create_stmt)

        # 3) Insert all rows
        for row in rows:
            keys = list(row.keys())
            vals = [row[k] for k in keys]
            insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, keys)),
                sql.SQL(", ").join(sql.Placeholder() * len(keys)),
            )
            cur.execute(insert, vals)

    conn.commit()
    print(f"Table '{table_name}' recreated and loaded with {len(rows)} rows.")


def main():
    parser = argparse.ArgumentParser(
        description="Recreate a Postgres table from a JSON file."
    )
    parser.add_argument(
        "json_file",
        help="Path to the JSON file (must contain an array under a single top-level key).",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="The top-level key in the JSON and the name for the Postgres table.",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Postgres DSN, e.g. postgresql://user:pass@host:port/dbname",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Database schema",
    )
    args = parser.parse_args()

    # Load JSON
    try:
        with open(args.json_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON file: {e}", file=sys.stderr)
        sys.exit(1)

    if args.table not in data or not isinstance(data[args.table], list):
        print(
            f"JSON must contain a list under the key '{args.table}'.", file=sys.stderr
        )
        sys.exit(1)

    # Connect to Postgres
    try:
        conn = psycopg2.connect(args.db)
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {args.schema}")
    except Exception as e:
        print(f"Error connecting to Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        recreate_table_from_json(conn, args.table, data[args.table])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
