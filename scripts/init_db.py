"""
Database Initialization Script - Creates all tables and indexes in Supabase PostgreSQL.
Usage:
    python -m scripts.init_db
"""

import asyncio
import sys
from sqlalchemy import text
from app.core.config import settings
from app.db.session import async_engine
from app.db.models import Base


async def init_database() -> None:
    """Creates all database tables defined in SQLAlchemy ORM models."""
    print("==================================================================")
    print("[INIT] Initializing Multi-Tenant Enterprise Database...")
    print(f"[CONFIG] Environment: {settings.ENVIRONMENT}")
    print("==================================================================")

    try:
        async with async_engine.begin() as conn:
            # Enable UUID extension if available
            print("[EXT] Ensuring uuid-ossp extension exists...")
            try:
                await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'))
            except Exception as ext_err:
                print(f"[NOTE] Extension notice (safe if UUIDs generated in python): {ext_err}")

            # Create all tables
            print("[TABLES] Creating ORM tables (Tenants, Users, Documents, Chunks, Memory, Chat, Audit)...")
            await conn.run_sync(Base.metadata.create_all)
            print("[SUCCESS] All tables created successfully!")

        print("\n[DONE] Database initialization completed successfully!")

    except Exception as e:
        print(f"\n[ERROR] Database initialization failed: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    asyncio.run(init_database())
