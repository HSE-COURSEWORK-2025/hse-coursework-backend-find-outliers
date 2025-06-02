import asyncio
import argparse
import logging
import re
import sys
from datetime import datetime
from collections import defaultdict

import numpy as np
from sqlalchemy.future import select
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from notifications import notifications_api
from db.schemas import RawRecords, OutliersRecords
from settings import Settings
from db.db_session import get_session

from redis import redis_client  # ваш синглтон-клиент aioredis

# Настройки
settings = Settings()
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
TARGET_EMAIL = ""
REDIS_KEY = ""

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def detect_outliers_zscore(values: list[float], threshold: float = 3.0) -> np.ndarray:
    arr = np.array(values, dtype=np.float64)
    mean = arr.mean()
    std = arr.std()
    if std == 0:
        return np.zeros(len(arr), dtype=bool)
    z_scores = np.abs((arr - mean) / std)
    return z_scores > threshold

async def detect_and_store_outliers(
    session: AsyncSession,
    target_email: str,
    iteration_num: int
) -> dict:
    global TARGET_EMAIL
    global REDIS_KEY
    logger.info(f"Iteration {iteration_num}: starting outlier detection for {target_email}")

    # 1) читаем записи
    result = await session.execute(
        select(RawRecords).where(RawRecords.email == target_email)
    )
    records = result.scalars().all()
    total_records = len(records)
    logger.info(f"Fetched {total_records} raw records for {target_email}")

    # 2) группируем по data_type
    grouped: dict[str, list[tuple[RawRecords, float]]] = defaultdict(list)
    for record in records:
        try:
            val = float(record.value)
        except ValueError:
            logger.warning(f"Skipping non-numeric value in record id={record.id}")
            continue
        grouped[record.data_type].append((record, val))
    logger.info(f"Grouped records into {len(grouped)} data types")

    now = datetime.utcnow()
    outliers_to_add: list[OutliersRecords] = []
    per_type_counts: dict[str, int] = {}

    # 3) ищем выбросы
    for data_type, rec_vals in grouped.items():
        await redis_client.set(REDIS_KEY, "true")
        recs, vals = zip(*rec_vals)
        mask = detect_outliers_zscore(list(vals))
        cnt = int(mask.sum())
        per_type_counts[data_type] = cnt
        logger.info(f"DataType '{data_type}': found {cnt} outliers out of {len(vals)} records")
        for rec, is_out in zip(recs, mask):
            if is_out:
                outliers_to_add.append(
                    OutliersRecords(
                        raw_record_id=rec.id,
                        outliers_search_iteration_num=iteration_num,
                        outliers_search_iteration_datetime=now,
                    )
                )

    # 4) сохраняем
    session.add_all(outliers_to_add)
    await session.commit()
    logger.info(f"Iteration {iteration_num}: committed {len(outliers_to_add)} outliers to DB")

    return {
        "total_records": total_records,
        "total_outliers": len(outliers_to_add),
        "per_type": per_type_counts,
        "started_at": now
    }

async def main(target_email: str):
    # 1) Подключаемся к Redis
    global TARGET_EMAIL
    global REDIS_KEY

    TARGET_EMAIL = target_email
    REDIS_KEY = f"{settings.REDIS_FIND_OUTLIERS_JOB_IS_ACTIVE_NAMESPACE}{target_email}"
    try:
        await redis_client.connect()
    except Exception as e:
        logger.error(f"Не удалось подключиться к Redis: {e}")
        sys.exit(1)

    try:
        # 2) Устанавливаем флаг "в работе"
        await redis_client.set(REDIS_KEY, "true", ex=3600)
        logger.info(f"Redis key {REDIS_KEY} set to true")

        # 3) Вся логика работы в рамках одной сессии
        async with get_session() as session:
            # определяем итерацию
            result = await session.execute(
                select(func.max(OutliersRecords.outliers_search_iteration_num))
                .join(RawRecords, OutliersRecords.raw_record_id == RawRecords.id)
                .where(RawRecords.email == target_email)
            )
            max_iter = result.scalar()
            iteration_number = (max_iter or 0) + 1
            logger.info(f"Next outlier iteration number: {iteration_number}")

            # уведомление о старте
            start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
            subject_start = f"[Iteration #{iteration_number}] Запуск поиска выбросов"
            body_start = f"""
            <html><body>
              <h2>🔍 Итерация #{iteration_number} — Запуск</h2>
              <p><strong>Пользователь:</strong> {target_email}</p>
              <p><strong>Время запуска:</strong> {start_time}</p>
              <p>Начинаем сканирование всех записей для поиска выбросов.</p>
            </body></html>
            """
            await notifications_api.send_email(target_email, subject_start, body_start)
            logger.info("Sent start notification email")

            # поиск и сохранение выбросов
            summary = await detect_and_store_outliers(session, target_email, iteration_number)

            # уведомление о завершении
            finish_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
            rows = "".join(
                f"<tr><td>{dt}</td><td style='text-align:center'>{cnt}</td></tr>"
                for dt, cnt in summary["per_type"].items()
            )
            subject_end = f"[Iteration #{iteration_number}] Завершение поиска выбросов"
            body_end = f"""
            <html><body>
              <h2>✅ Итерация #{iteration_number} завершена</h2>
              <p><strong>Пользователь:</strong> {target_email}</p>
              <p><strong>Время старта:</strong> {start_time}</p>
              <p><strong>Время окончания:</strong> {finish_time}</p>
              <p><strong>Всего записей проверено:</strong> {summary['total_records']}</p>
              <p><strong>Найдено выбросов:</strong> {summary['total_outliers']}</p>
              <h3>Расчёт по типам данных</h3>
              <table border="1" cellpadding="5" cellspacing="0">
                <thead><tr><th>Тип данных</th><th>Выбросов</th></tr></thead>
                <tbody>{rows}</tbody>
              </table>
            </body></html>
            """
            await notifications_api.send_email(target_email, subject_end, body_end)
            logger.info("Sent completion notification email")

    finally:
        # 4) Сброс флага и отключение
        try:
            await redis_client.set(REDIS_KEY, "false")
            logger.info(f"Redis key {REDIS_KEY} set to false")
        except Exception as e:
            logger.error(f"Не удалось сбросить Redis key {REDIS_KEY}: {e}")
        await redis_client.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run outliers detection for a given user email."
    )
    parser.add_argument(
        "--email", "-e",
        dest="email",
        required=True,
        help="Email address of the user whose records will be processed"
    )
    args = parser.parse_args()

    if not EMAIL_REGEX.fullmatch(args.email):
        logger.error(f"Invalid email format: {args.email}")
        sys.exit(1)

    asyncio.run(main(args.email))
