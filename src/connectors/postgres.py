import os
from typing import Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://htap_user:htap_pass@localhost:5433/htap_database"
)

# Engine configurado com pooling e timeout defensivo
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db_session() -> Generator[Session, None, None]:
    """Dependency para obter sessão de banco relacional com fechamento automático."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
