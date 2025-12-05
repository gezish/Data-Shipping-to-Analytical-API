# fastapi_app/db.py
import os
import asyncio
import asyncpg
from typing import Optional
from dotenv import load_dotenv

# Load .env at startup
load_dotenv()

POOL: Optional[asyncpg.pool.Pool] = None


async def init_db_pool():
    """
    Initialize asyncpg connection pool using environment variables.
    Ensures credentials are never hard-coded and supports Docker overrides.
    """
    global POOL
    if POOL:
        return POOL

    PG = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "database": os.getenv("POSTGRES_DB"),
    }

    # Optional: validate environment variables
    missing = [k for k, v in PG.items() if v is None]
    if missing:
        raise RuntimeError(f"Missing DB environment variables: {missing}")

    POOL = await asyncpg.create_pool(
        **PG,
        min_size=1,
        max_size=10,
    )
    return POOL


async def close_db_pool():
    """Close the DB pool on FastAPI shutdown."""
    global POOL
    if POOL:
        await POOL.close()
        POOL = None
