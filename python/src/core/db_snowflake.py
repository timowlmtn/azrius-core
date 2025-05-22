# web/app/core/db_snowflake.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from core.SnowflakeConnector import SnowflakeConnector


def connect_snowflake():
    sf_connector = SnowflakeConnector()
    return sf_connector.connect()


def _create_engine() -> Engine:

    return create_engine("snowflake://", creator=connect_snowflake)


# Create SQLAlchemy engine and session
engine = _create_engine()  # Set echo=False to disable SQL logs
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

if __name__ == "__main__":
    engine = _create_engine()
    with engine.connect() as conn:
        conn.execute(
            text(
                """
            INSERT INTO web.azrius_user (
                first_name, last_name, email, phone_number, llm_api_key, app_api_key
            ) VALUES (
                'Testy', 'Testface', 'test@example.com', '555-0000', 'key', 'app-key'
            )
        """
            )
        )
        conn.commit()
