import os
from typing import Generator
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

load_dotenv()


def _build_default_database_url() -> str:
    """Build the default PostgreSQL connection URL from environment variables."""
    user = os.getenv("POSTGRES_USER", "comex_user")
    password = os.getenv("POSTGRES_PASSWORD", "comex_pass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5434")
    db_name = os.getenv("POSTGRES_DB", "comex_fiduciary_db")
    return f"postgresql+psycopg://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db_name}"


def _normalize_sqlalchemy_url(url: str) -> str:
    """Ensure SQLAlchemy uses the psycopg3 driver for PostgreSQL URLs."""
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://") :]
    return url


DATABASE_URL = _normalize_sqlalchemy_url(os.getenv("DATABASE_URL") or _build_default_database_url())

# Engine configurado com pooling e timeout defensivo
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
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
