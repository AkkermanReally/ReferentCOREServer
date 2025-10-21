import redis.asyncio as redis
import logging
import os

logger = logging.getLogger(__name__)
redis_client = None


async def init_redis_pool():
    global redis_client
    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_client = redis.from_url(
            f"redis://{redis_host}", encoding="utf-8", decode_responses=True
        )
        await redis_client.ping()
        logger.info(f"Успешное подключение к Redis ({redis_host}).")
    except Exception as e:
        logger.error(
            f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось подключиться к Redis. {e}", exc_info=True
        )
        exit(1)


def get_redis_client() -> redis.Redis:
    return redis_client
