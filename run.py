import asyncio
import argparse
import logging
import re
import sys
from datetime import datetime
from collections import defaultdict

import numpy as np
from sqlalchemy.future import select
from sqlalchemy import func, insert, cast, Numeric
from sqlalchemy.ext.asyncio import AsyncSession

from notifications import notifications_api
from db.schemas import RawRecords, OutliersRecords, ProcessedRecords, ProcessedRecordsOutliersRecords
from settings import Settings
from db.db_session import get_session

from redis import redis_client  # ваш синглтон-клиент aioredis

# Настройки
settings = Settings()
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

async def send_outlier_start_notification(
    email: str, iteration_number: int, start_time: str
):
    subject = f"[Iteration #{iteration_number}] Запуск поиска выбросов"
    body = f"""
    <html><body>
      <h2>🔍 Итерация #{iteration_number} — Запуск</h2>
      <p><strong>Пользователь:</strong> {email}</p>
      <p><strong>Время запуска:</strong> {start_time}</p>
      <p>Начинаем сканирование всех записей для поиска выбросов.</p>
    </body></html>
    """
    await notifications_api.send_email(email, subject, body)
    logger.info("Sent start notification email")


async def send_outlier_completion_notification(
    email: str,
    iteration_number: int,
    start_time: str,
    finish_time: str,
    raw_summary: dict,
    processed_summary: dict
):
    # Формируем таблицы для raw и processed
    raw_rows = "".join(
        f"<tr><td>{dt}</td><td style='text-align:center'>{cnt}</td></tr>"
        for dt, cnt in raw_summary["per_type"].items()
    )
    proc_rows = "".join(
        f"<tr><td>{dt}</td><td style='text-align:center'>{cnt}</td></tr>"
        for dt, cnt in processed_summary["per_type"].items()
    )
    subject = f"[Iteration #{iteration_number}] Завершение поиска выбросов"
    body = f"""
    <html><body>
      <h2>✅ Итерация #{iteration_number} завершена</h2>
      <p><strong>Пользователь:</strong> {email}</p>
      <p><strong>Время начала:</strong> {start_time}</p>
      <p><strong>Время окончания:</strong> {finish_time}</p>

      <h3>Raw Records</h3>
      <p><strong>Всего проверено записей:</strong> {raw_summary['total_records']}</p>
      <p><strong>Найдено выбросов:</strong> {raw_summary['total_outliers']}</p>
      <table border="1" cellpadding="5" cellspacing="0">
        <thead><tr><th>Тип данных</th><th>Выбросов</th></tr></thead>
        <tbody>{raw_rows}</tbody>
      </table>

      <h3>Processed Records</h3>
      <p><strong>Всего проверено записей:</strong> {processed_summary['total_records']}</p>
      <p><strong>Найдено выбросов:</strong> {processed_summary['total_outliers']}</p>
      <table border="1" cellpadding="5" cellspacing="0">
        <thead><tr><th>Тип данных</th><th>Выбросов</th></tr></thead>
        <tbody>{proc_rows}</tbody>
      </table>

    </body></html>
    """
    await notifications_api.send_email(email, subject, body)
    logger.info("Sent completion notification email")


def detect_outliers_zscore(values: list[float], threshold: float = 3.0) -> np.ndarray:
    arr = np.array(values, dtype=np.float64)
    mean = arr.mean()
    std = arr.std()
    if std == 0:
        return np.zeros(len(arr), dtype=bool)
    z_scores = np.abs((arr - mean) / std)
    return z_scores > threshold

async def detect_and_store_raw_outliers(
    session: AsyncSession,
    target_email: str,
    iteration_num: int
) -> dict:
    logger.info(f"Iteration {iteration_num}: starting outlier detection for {target_email}")
    
    result = await session.execute(
        select(RawRecords).where(RawRecords.email == target_email)
    )
    records = result.scalars().all()
    total_records = len(records)
    grouped = defaultdict(list)
    for record in records:
        try:
            val = float(record.value)
        except ValueError:
            logger.warning(f"Skipping non-numeric value in record id={record.id}")
            continue
        grouped[record.data_type].append((record, val))
    
    now = datetime.utcnow()
    outliers_to_add = []
    per_type_counts = {}

    for data_type, rec_vals in grouped.items():
        await redis_client.set(f"{settings.REDIS_FIND_OUTLIERS_JOB_IS_ACTIVE_NAMESPACE}{target_email}", "true")
        recs, vals = zip(*rec_vals)
        mask = detect_outliers_zscore(list(vals), threshold=3)
        per_type_counts[data_type] = int(mask.sum())
        for rec, is_out in zip(recs, mask):
            if is_out:
                outliers_to_add.append(
                    OutliersRecords(
                        raw_record_id=rec.id,
                        outliers_search_iteration_num=iteration_num,
                        outliers_search_iteration_datetime=now,
                    )
                )

    session.add_all(outliers_to_add)
    await session.commit()
    logger.info(f"Iteration {iteration_num}: committed {len(outliers_to_add)} outliers to DB")

    return {
        "total_records": total_records,
        "total_outliers": len(outliers_to_add),
        "per_type": per_type_counts,
    }


async def detect_and_store_processed_outliers(
    session: AsyncSession,
    target_email: str,
    iteration_num: int
) -> dict:
    """
    То же самое, что detect_and_store_outliers, но для ProcessedRecords:
    - Берём все ProcessedRecords для пользователя
    - Группируем по data_type
    - Находим выбросы по z-score
    - Сохраняем ссылки на processed_record_id в таблицу ProcessedRecordsOutliersRecords
    """
    logger.info(f"Iteration {iteration_num}: starting processed-records outlier detection for {target_email}")

    # 1) читаем предобработанные записи
    result = await session.execute(
        select(ProcessedRecords).where(ProcessedRecords.email == target_email)
    )
    records = result.scalars().all()
    total_records = len(records)

    # 2) группируем по data_type
    grouped = defaultdict(list)
    for rec in records:
        try:
            val = float(rec.value)
        except ValueError:
            logger.warning(f"Skipping non-numeric value in processed record id={rec.id}")
            continue
        grouped[rec.data_type].append((rec, val))

    now = datetime.utcnow()
    per_type_counts = {}
    to_add = []

    # 3) ищем выбросы и готовим объекты для вставки
    for data_type, rec_vals in grouped.items():
        recs, vals = zip(*rec_vals)
        mask = detect_outliers_zscore(list(vals), threshold=2)
        cnt = int(mask.sum())
        per_type_counts[data_type] = cnt
        logger.info(f"[Processed] DataType '{data_type}': found {cnt} outliers out of {len(vals)}")

        for rec, is_out in zip(recs, mask):
            if is_out:
                to_add.append(
                    ProcessedRecordsOutliersRecords(
                        processed_record_id=rec.id,
                        outliers_search_iteration_num=iteration_num,
                        outliers_search_iteration_datetime=now,
                    )
                )

    # 4) сохраняем все новые связи
    session.add_all(to_add)
    await session.commit()
    logger.info(f"Iteration {iteration_num}: committed {len(to_add)} processed-records outliers")

    return {
        "total_records": total_records,
        "total_outliers": len(to_add),
        "per_type": per_type_counts
    }


async def main(target_email: str):
    # Подключение к Redis и установка флага
    redis_key = f"{settings.REDIS_FIND_OUTLIERS_JOB_IS_ACTIVE_NAMESPACE}{target_email}"
    await redis_client.connect()
    await redis_client.set(redis_key, "true", ex=3600)

    async with get_session() as session:
        # Итерация
        result = await session.execute(
            select(func.max(OutliersRecords.outliers_search_iteration_num))
            .join(RawRecords, OutliersRecords.raw_record_id == RawRecords.id)
            .where(RawRecords.email == target_email)
        )
        max_iter = result.scalar() or 0
        iteration_number = max_iter + 1

        start_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        try:
            await send_outlier_start_notification(target_email, iteration_number, start_time)
        except Exception as e:
            logger.error('failed to send notification')

        raw_summary = await detect_and_store_raw_outliers(session, target_email, iteration_number)
        processed_summary = await detect_and_store_processed_outliers(
            session, target_email, iteration_number
        )

        finish_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        try:
            await send_outlier_completion_notification(
                target_email,
                iteration_number,
                start_time,
                finish_time,
                raw_summary,
                processed_summary
            )
        except Exception as e:
            logger.error('failed to send notification')

    # Сброс флага и отключение
    await redis_client.set(redis_key, "false")
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
