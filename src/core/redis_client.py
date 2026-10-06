from redis import asyncio as redis
from src.core.config import settings

r = redis.Redis(
    host=settings.REDIS_HOST, 
    port=settings.REDIS_PORT, 
    decode_responses=True
)
