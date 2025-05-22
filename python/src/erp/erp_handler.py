import os
from sqlalchemy import text
from core.SingletonLogger import SingletonLogger

if os.getenv("DB_TYPE") == "postgres":
    from core.db_postgres import SessionLocal
elif os.getenv("DB_TYPE") == "snowflake":
    from core.db_snowflake import SessionLocal
else:
    raise ValueError("Unsupported DB_TYPE. Set DB_TYPE to 'postgres' or 'snowflake'.")

logger = SingletonLogger("erp").get_logger()


def get_tables_in_schema(schema_name: str) -> list[str]:
    """
    Return a list of table names in the specified schema.
    Works for both PostgreSQL and Snowflake.
    """
    with SessionLocal() as db:
        if os.getenv("DB_TYPE") == "postgres":
            sql = text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_type = 'BASE TABLE';
                """
            )
            result = db.execute(sql, {"schema_name": schema_name})
            tables = [row[0] for row in result]
        else:  # snowflake
            sql = text(f"SHOW TABLES IN SCHEMA {schema_name}")
            result = db.execute(sql)
            tables = [row["name"] for row in result]

        logger.info(f"Fetched {len(tables)} tables from schema '{schema_name}'")
        return tables


def get_table_rows(schema_name: str, table_name: str, limit: int = 100) -> list[dict]:
    """
    Return up to `limit` rows from the given table in the given schema,
    as a list of dicts (column_name -> value).
    """
    db_type = os.getenv("DB_TYPE")
    full_name = f"{schema_name}.{table_name}"
    logger.info(f"Fetching up to {limit} rows from {full_name}")

    with SessionLocal() as db:
        if db_type == "postgres":
            # In Postgres we can parametrize the LIMIT but not identifiers
            sql = text(f"SELECT * FROM {full_name} LIMIT :limit")
            result = db.execute(sql, {"limit": limit})

        elif db_type == "snowflake":
            # Snowflake doesn’t support a bind param for LIMIT, so inline it
            sql = text(f"SELECT * FROM {full_name} LIMIT {limit}")
            result = db.execute(sql)

        else:
            raise ValueError("Unsupported DB_TYPE.")

        # SQLAlchemy Row objects support ._mapping to get a dict-like view
        rows = [dict(row._mapping) for row in result]
        logger.info(f"Retrieved {len(rows)} rows from {full_name}")
        return rows


if __name__ == "__main__":
    schema = "smith_street_barbershop"
    tables = get_tables_in_schema(schema)
    print("Tables:", tables)

    # example fetch
    for table in tables:
        sample = get_table_rows(schema, table, limit=5)
        print(f"First 5 rows from {tables[0]}:", sample)
