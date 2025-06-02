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
        # 1) Формируем URL с драйвером asyncpg
        #    Например: postgresql+asyncpg://user:password@host:port/dbname
        self.url = (
            f"{settings.DB_ENGINE}+asyncpg://"
            f"{settings.DB_USER}:{settings.DB_PASSWORD}"
            f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        # 2) Создаём асинхронный движок
        self.engine = create_async_engine(
            self.url,
            echo=False,           # или True, если хотите логировать все SQL-запросы
            pool_pre_ping=True,   # чтобы проверять «жив ли» соединение
        )
        # 3) Настраиваем sessionmaker для AsyncSession
        self._session_factory = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,  # чтобы объекты не «протухали» после commit()
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
        # Открываем асинхронную сессию
        async with self.create_session() as session:
            # Начинаем транзакцию
            async with session.begin():
                try:
                    result: Result = await session.execute(db_request)
                    # Если нужно получить все строки:
                    rows = result.fetchall()
                except Exception:
                    # В случае ошибки, транзакция автоматически откатится
                    raise
                else:
                    return rows


# Инстанс асинхронного движка
db_engine = AsyncDbEngine()


async def db_engine_check():
    """
    Проверка на подключение к базе: делаем SELECT version();
    """
    logger.info(f"Connecting to database {settings.DB_HOST}:{settings.DB_PORT}")
    try:
        # text() — это SQLAlchemy text clause
        version_row = await db_engine.request(text("SELECT version();"))
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    # version_row — список кортежей, например [(version_string,)]
    version_info = version_row[0][0] if version_row else "Unknown"
    logger.info(f"Database version: {version_info}")
