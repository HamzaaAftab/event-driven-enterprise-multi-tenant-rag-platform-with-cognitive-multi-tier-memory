"""
Async Database Session & Engine Configuration using SQLAlchemy 2.0 and asyncpg.
"""

import urllib.parse
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from app.core.config import settings

# Normalize the Database URL for asyncpg and safely encode password
raw_db_url = settings.DATABASE_URL
if raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif raw_db_url.startswith("postgresql://") and not raw_db_url.startswith("postgresql+asyncpg://"):
    raw_db_url = raw_db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

# Safely encode special characters in password (e.g. #, +, @)
parsed = urllib.parse.urlsplit(raw_db_url)
if parsed.password:
    encoded_pass = urllib.parse.quote_plus(urllib.parse.unquote(parsed.password))
    netloc = f"{parsed.username}:{encoded_pass}@{parsed.hostname}:{parsed.port}"
    db_url = urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
else:
    db_url = raw_db_url

# Create Async Engine with production connection pool settings and pgbouncer compatibility
async_engine: AsyncEngine = create_async_engine(
    db_url,
    echo=settings.DEBUG,
    future=True,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
    },
)

# Async Session Factory
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)
async_session_factory = AsyncSessionLocal


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI Dependency to inject an asynchronous database session.
    Automatically commits on success or rollbacks on exception, then closes session.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
