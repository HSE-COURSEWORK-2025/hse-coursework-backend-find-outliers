import logging
from typing import Any, Union, List

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.engine import Result

from .settings import settings

logger = logging.getLogger("database")


class AsyncDbEngine:
    def __init__(self):
        self.url = (
            f"{settings.DB_ENGINE}+asyncpg://"
            f"{settings.DB_USER}:{settings.DB_PASSWORD}"
            f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        self.engine = create_async_engine(
            self.url,
            echo=False,
            pool_pre_ping=True,
        )
        self._session_factory = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    def create_session(self) -> AsyncSession:
        """
        Возвращает новый AsyncSession (без открытия транзакции).
        Для работы с БД:
            async with db_engine.create_session() as session:
                ... await session.execute(...) ...
        """
        return self._session_factory()

    async def request(
        self,
        db_request: Union[str, Any],
    ) -> List:
        """
        Асинхронно выполняет произвольный SQL-запрос (например, text("SELECT ..."))
        и возвращает список результатов (fetchall).

        Пример использования:
            rows = await db_engine.request(text("SELECT * FROM users"))
            for row in rows:
                print(row)
        """
        async with self.create_session() as session:
            async with session.begin():
                try:
                    result: Result = await session.execute(db_request)
                    rows = result.fetchall()
                except Exception:
                    raise
                else:
                    return rows


db_engine = AsyncDbEngine()


async def db_engine_check():
    """
    Проверка на подключение к базе: делаем SELECT version();
    """
    logger.info(f"Connecting to database {settings.DB_HOST}:{settings.DB_PORT}")
    try:
        version_row = await db_engine.request(text("SELECT version();"))
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    version_info = version_row[0][0] if version_row else "Unknown"
    logger.info(f"Database version: {version_info}")
