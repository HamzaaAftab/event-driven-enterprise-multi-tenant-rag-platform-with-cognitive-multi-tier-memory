"""Check live database tables in Supabase PostgreSQL."""
import asyncio
from sqlalchemy import text
from app.db.session import async_engine

async def main():
    async with async_engine.connect() as conn:
        res = await conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))
        tables = [row[0] for row in res.fetchall()]
        print("\n[LIVE SUPABASE TABLES IN PUBLIC SCHEMA]:")
        for t in tables:
            print(f"  -> {t}")

if __name__ == "__main__":
    asyncio.run(main())
