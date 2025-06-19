import aioredis
import logging
from settings import settings

logger = logging.getLogger(__name__)


class RedisClient:
    _instance = None
    _redis = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
        return cls._instance

    async def connect(self):
        """
        Подключение к Redis. Если клиент ещё не проинициализирован, создаём его.
        """
        if self._redis is None:
            try:
                self._redis = await aioredis.from_url(
                    f"redis://{settings.REDIS_HOST}",
                    decode_responses=True,
                )
                logger.info(f"Подключение к Redis: redis://{settings.REDIS_HOST}")
            except Exception as e:
                logger.error(f"Ошибка подключения к Redis: {e}")
                raise

    async def disconnect(self):
        """
        Отключение от Redis и сброс клиента.
        """
        if self._redis:
            await self._redis.close()
            logger.info("Отключение от Redis.")
            self._redis = None

    def __getattr__(self, name):
        """
        Перенаправление всех запросов (кроме явно определённых методов)
        к объекту redis. Если redis не подключён, генерируется исключение.
        """
        if self._redis is None:
            raise Exception(
                "Redis не подключен. Вызовите connect() перед использованием."
            )
        return getattr(self._redis, name)

    def __repr__(self):
        return f"<RedisClient connected={self._redis is not None}>"


redis_client: aioredis.Redis = RedisClient()
