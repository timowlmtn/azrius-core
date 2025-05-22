import os
from sqlalchemy import text  # ✅ Required for raw SQL


def load_db():
    db_type = os.getenv("DB_TYPE", "mock").lower()

    if db_type == "postgres":
        from core.db_postgres import Base, SessionLocal
    elif db_type == "snowflake":
        from core.db_snowflake import Base, SessionLocal
    elif db_type == "mock":
        from core.db_mock import Base, SessionLocal
    else:
        raise ValueError(
            "Unsupported DB_TYPE. Set DB_TYPE to 'postgres', 'snowflake', or 'mock'."
        )

    return Base, SessionLocal


def debug_connection():
    (Base, SessionLocal) = load_db()
    dialect = os.getenv("DB_TYPE", "mock").lower()

    with SessionLocal() as db:
        try:
            if dialect == "snowflake":
                result = db.execute(
                    text(
                        "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_USER(), CURRENT_WAREHOUSE()"
                    )
                ).fetchone()
                print("🔍 Snowflake Connection Debug Info:")
                print(f"  Database:  {result[0]}")
                print(f"  Schema:    {result[1]}")
                print(f"  User:      {result[2]}")
                print(f"  Warehouse: {result[3]}")
            else:
                print(f"🔍 {dialect.title()} Connection Debug Info:")

                result = db.execute(text("SELECT current_database()")).fetchone()
                print(f"  Database:  {result[0]}")

                if dialect == "postgresql":
                    result = db.execute(
                        text("SELECT current_schema(), current_user")
                    ).fetchone()
                    print(f"  Schema:    {result[0]}")
                    print(f"  User:      {result[1]}")
                elif dialect == "mysql":
                    print("  Schema:    [MySQL does not support `current_schema()`]")
                    print("  User:      [Check session variables if needed]")
        except Exception as e:
            print("❌ Failed to execute debug query.")
            print(f"Error: {e}")


if __name__ == "__main__":
    debug_connection()
