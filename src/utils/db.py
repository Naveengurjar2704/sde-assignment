from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from src.config import settings

engine = create_async_engine(settings.DATABASE_URL, pool_size=20, max_overflow=10)
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


@asynccontextmanager
async def get_db_session() -> AsyncSession:
    """
    Async context manager for use outside FastAPI dependency injection
    (e.g., in Celery tasks and background workers).

    Usage:
        async with get_db_session() as session:
            await session.execute(...)
            await session.commit()
    """
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
