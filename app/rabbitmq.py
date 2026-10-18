import aio_pika
import json
import logging
from typing import Optional
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractRobustChannel,
    AbstractRobustExchange,
)

from app.config import settings

logger = logging.getLogger(__name__)


class RabbitMQClient:
    def __init__(self) -> None:
        self.connection: Optional[AbstractRobustConnection] = None
        self.channel: Optional[AbstractRobustChannel] = None
        self.exchange: Optional[AbstractRobustExchange] = None

    async def connect(self) -> None:
        """Подключение к RabbitMQ и создание exchange/queue"""
        self.connection = await aio_pika.connect_robust(settings.rabbitmq.url)
        self.channel = await self.connection.channel()

        self.exchange = await self.channel.declare_exchange(
            settings.rabbitmq.exchange,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        queue = await self.channel.declare_queue(
            settings.rabbitmq.queue,
            durable=True,
        )

        await queue.bind(self.exchange or settings.rabbitmq.exchange, routing_key="new")

        logger.info(
            f"Connected to RabbitMQ. "
            f"Exchange: {settings.rabbitmq.exchange}, "
            f"Queue: {settings.rabbitmq.queue}"
        )

    async def publish_payment_event(self, payment_id: str, payment_data: dict) -> None:
        """Публикация события о создании платежа"""
        if self.channel is None or self.exchange is None:
            raise RuntimeError("RabbitMQ client not connected. Call connect() first.")

        message_body = json.dumps(
            {"event_type": "payment.created", "payment_id": payment_id, **payment_data}
        ).encode()

        message = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        await self.exchange.publish(
            message,
            routing_key="new",
        )
        logger.info(f"Payment event published to RabbitMQ: {payment_id}")

    async def close(self) -> None:
        """Закрытие соединения при остановке приложения"""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("RabbitMQ connection closed")


rabbitmq_client = RabbitMQClient()
