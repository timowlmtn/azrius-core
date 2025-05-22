from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

import os

# PostgreSQL's connection URL — change these values to match your environment
DB_USER = os.getenv("POSTGRES_USER", "azrius_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "azrius_password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "azrius")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL, echo=True)  # Set echo=False to disable SQL logs
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
metadata = MetaData(schema="web")
Base = declarative_base(metadata=metadata)


def get_db_url():
    """
    Returns the database URL for SQLAlchemy.
    """
    return DATABASE_URL


def _create_engine() -> Engine:
    user = os.getenv("POSTGRES_USER", "your_user")
    password = os.getenv("POSTGRES_PASSWORD", "your_password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "your_database")
    schema = os.getenv("POSTGRES_SCHEMA", "public")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, connect_args={"options": f"-csearch_path={schema}"})


def connect() -> SessionLocal:
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "azrius"),
        user=os.getenv("POSTGRES_USER", "azrius_user"),
        password=os.getenv("POSTGRES_PASSWORD", "azrius_password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
