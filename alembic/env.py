"""Alembic migration environment for Comex Fiduciary Engine V2."""

from logging.config import fileConfig
import os

from alembic import context
from sqlalchemy import engine_from_config, pool

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional in minimal migration containers
    load_dotenv = None

DEFAULT_DATABASE_URL = (
    "postgresql+psycopg://comex_user:comex_pass@localhost:5434/comex_fiduciary_db"
)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def get_database_url() -> str:
    """Resolve DATABASE_URL with psycopg3 driver normalization."""
    if load_dotenv is not None:
        load_dotenv()

    raw_url = (
        os.getenv("DATABASE_URL")
        or config.get_main_option("sqlalchemy.url")
        or DEFAULT_DATABASE_URL
    ).strip()

    if raw_url.startswith("postgresql://"):
        return "postgresql+psycopg://" + raw_url.removeprefix("postgresql://")
    if raw_url.startswith("postgres://"):
        return "postgresql+psycopg://" + raw_url.removeprefix("postgres://")
    return raw_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
