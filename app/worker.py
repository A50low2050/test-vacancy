import asyncio
from datetime import datetime, timezone
from sqlalchemy import select

from app.consumer import app, broker, async_session_factory
from app.models import Outbox, OutboxStatus
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def outbox_publisher_task() -> None:
    """
    Фоновая задача: читает непубликованные события из Outbox
    и отправляет их в RabbitMQ.
    """
    logger.info("Запущен фоновый паблишер Outbox событий.")
    while True:
        try:
            async with async_session_factory() as session:
                stmt = (
                    select(Outbox)
                    .where(Outbox.status == OutboxStatus.PENDING)
                    .limit(10)
                )
                result = await session.execute(stmt)
                records = result.scalars().all()

                for record in records:
                    try:

                        await broker.publish(
                            record.payload,
                            exchange=settings.rabbitmq.exchange,
                            routing_key=settings.rabbitmq.queue,
                        )

                        record.status = OutboxStatus.PUBLISHED
                        record.published_at = datetime.now(timezone.utc)
                        await session.commit()
                        logger.info(
                            f"Событие Outbox {record.id} успешно опубликовано в брокер."
                        )

                    except Exception as e:
                        logger.error(f"Ошибка публикации Outbox {record.id}: {e}")
                        await session.rollback()

                        record.attempts += 1
                        await session.commit()

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Критическая ошибка в outbox_publisher_task: {e}")
            await asyncio.sleep(5)


async def main() -> None:
    logger.info("Воркер обработки платежей запущен и готов к работе.")

    publisher_task = asyncio.create_task(outbox_publisher_task())

    try:
        await app.run()
    finally:
        publisher_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
