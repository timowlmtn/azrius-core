from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine

# In-memory SQLite for testing
engine = create_engine("sqlite:///:memory:", echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
