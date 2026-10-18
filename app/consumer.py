import uuid
import asyncio
import random

from datetime import datetime, timezone
from typing import Dict, Any

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitExchange, ExchangeType
from sqlalchemy import select

from app.config import settings
from app.database import async_session_factory
from app.models import Payment, PaymentStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)

broker = RabbitBroker(settings.rabbitmq.url)
app = FastStream(broker)


payment_exchange = RabbitExchange(
    name=settings.rabbitmq.exchange, type=ExchangeType.DIRECT
)
payment_queue = RabbitQueue(name=settings.rabbitmq.queue)

dlq_exchange = RabbitExchange(name=settings.rabbitmq.exchange, type=ExchangeType.DIRECT)
dlq_queue = RabbitQueue(name="payments.dlq")


async def emulate_payment_processing() -> bool:
    """Эмуляция обработки: 2-5 сек, 90% успех, 10% ошибка"""
    processing_time = random.uniform(2, 5)
    logger.info(f"Обработка платежа... (займет {processing_time:.2f}с)")
    await asyncio.sleep(processing_time)

    is_success = random.random() < 0.9
    if is_success:
        logger.info("Обработка платежа выполнена успешно")
    else:
        logger.warning("Обработка платежа завершилась ошибкой (симулированная ошибка)")

    return is_success


async def send_webhook_with_retry(
    payment_uuid: uuid.UUID,
    webhook_url: str,
    webhook_data: Dict[str, Any],
    max_attempts: int = 3,
) -> bool:
    """
    Отправка webhook уведомления с экспоненциальной задержкой при ошибках.
    Возвращает True при успешной отправке, False если все попытки исчерпаны.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(max_attempts):
            try:
                response = await client.post(
                    webhook_url,
                    json=webhook_data,
                    headers={"Content-Type": "application/json"},
                )

                if response.status_code in (200, 201, 202):
                    logger.info(f"Webhook успешно отправлен для {payment_uuid}")
                    return True
                else:
                    logger.error(
                        f"Ошибка отправки webhook, код ответа: {response.status_code}"
                    )

            except Exception as e:
                logger.error(f"Ошибка запроса webhook для {payment_uuid}: {e}")

            if attempt < max_attempts - 1:
                delay = 2**attempt
                logger.info(
                    f"Попытка повторной отправки {attempt + 2}/{max_attempts}. Ожидание {delay}с..."
                )
                await asyncio.sleep(delay)

    return False


@broker.subscriber(queue=payment_queue, exchange=payment_exchange)
async def consume_payment_event(message: Dict[str, Any]) -> None:
    """Основной обработчик сообщений из очереди payments.new"""
    payment_id_str = message.get("payment_id")

    if not payment_id_str:
        logger.error(
            "payment_id отсутствует в сообщении. Подтверждаем получение (ack)."
        )
        return

    try:
        payment_uuid = uuid.UUID(payment_id_str)
    except ValueError:
        logger.error(f"Неверный формат UUID в сообщении: {payment_id_str}")
        return

    logger.info(f"Получено событие платежа для обработки: {payment_uuid}")

    async with async_session_factory() as session:
        try:
            result = await session.execute(
                select(Payment).where(Payment.id == payment_uuid)
            )
            payment = result.scalar_one_or_none()

            if not payment:
                logger.error(
                    f"Платеж {payment_uuid} не найден в БД. Подтверждаем получение (ack)."
                )
                return

            processing_success = await emulate_payment_processing()
            new_status = (
                PaymentStatus.SUCCEEDED if processing_success else PaymentStatus.FAILED
            )

            payment.status = new_status
            await session.commit()
            logger.info(f"Статус платежа {payment_uuid} обновлен на {new_status.value}")

            webhook_data = {
                "event": "payment.processed",
                "payment_id": str(payment.id),
                "status": new_status.value,
                "amount": str(payment.amount),
                "currency": payment.currency.value,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            webhook_success = await send_webhook_with_retry(
                payment_uuid=payment_uuid,
                webhook_url=payment.webhook_url,
                webhook_data=webhook_data,
                max_attempts=3,
            )

            if not webhook_success:
                logger.error(
                    f"Все 3 попытки отправки webhook для {payment_uuid} исчерпаны. Отправка в DLQ."
                )
                await broker.publish(
                    message,
                    exchange=settings.rabbitmq.exchange,
                    routing_key="payments.dlq",
                )

            logger.info(f"Обработка платежа {payment_uuid} завершена")

        except Exception as e:
            logger.error(
                f"Критическая ошибка при обработке платежа {payment_uuid}: {e}"
            )
            await session.rollback()
            raise


@broker.subscriber(queue=dlq_queue, exchange=dlq_exchange)
async def consume_dlq_events(message: Dict[str, Any]) -> None:
    """Обработчик Dead Letter Queue"""
    payment_id = message.get("payment_id")
    logger.critical(
        f"DLQ: Платеж {payment_id} "
        f"окончательно не обработан после 3 попыток отправки webhook. Требуется ручная проверка."
    )
