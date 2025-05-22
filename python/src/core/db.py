# db.py (extended with main test)
import os
import argparse
from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_sqlalchemy_engine(db_type: str = None) -> Engine:
    """
    Returns a SQLAlchemy engine for either Postgres or Snowflake,
    based on the argument or the environment variable DB_TYPE.
    """
    db_type = (db_type or os.getenv("DB_TYPE", "postgres")).lower()

    if db_type == "postgres":
        from core import db_postgres

        return db_postgres._create_engine()
    if db_type == "snowflake":
        from core import db_snowflake

        return db_snowflake._create_engine()
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}")


def connect(db_type):
    """
    Connect to the database using the specified type.
    """
    if db_type == "postgres":
        from core import db_postgres

        return db_postgres.connect()
    elif db_type == "snowflake":
        from core import db_snowflake

        return db_snowflake.connect_snowflake()
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test database connection")
    parser.add_argument(
        "--db-type",
        type=str,
        default="postgres",
        choices=["postgres", "snowflake"],
        help="Type of database to connect to",
    )

    args = parser.parse_args()
    try:
        engine = get_sqlalchemy_engine(args.db_type)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT CURRENT_TIMESTAMP;"))
            print("Connection successful.")
            for row in result:
                print("Current time:", row[0])
    except Exception as e:
        print(f"Connection failed: {e}")
