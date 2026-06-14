import redis.asyncio as aioredis

from src.config import settings

redis_client: aioredis.Redis = aioredis.from_url(
    settings.REDIS_URL, decode_responses=True
)


async def get_redis() -> aioredis.Redis:
    return redis_client
