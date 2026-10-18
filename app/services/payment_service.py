import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException, status

from app.models import Payment, Outbox, OutboxStatus
from app.schemas import PaymentCreate


async def create_payment(
    session: AsyncSession, body: PaymentCreate, idempotency_key: str
) -> Payment:
    """
    Бизнес-логика создания платежа с гарантией доставки (Outbox pattern).
    """

    stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
    result = await session.execute(stmt)
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Payment with Idempotency-Key '{idempotency_key}' already exists",
        )

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_=body.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(body.webhook_url),
    )
    session.add(payment)

    await session.flush()

    outbox_record = Outbox(
        event_type="payment.created",
        aggregate_id=payment.id,
        payload={
            "payment_id": str(payment.id),
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "webhook_url": payment.webhook_url,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        status=OutboxStatus.PENDING,
        attempts=0,
    )
    session.add(outbox_record)

    await session.commit()
    await session.refresh(payment)

    return payment


async def get_payment_by_id(
    session: AsyncSession, payment_id: uuid.UUID
) -> Optional[Payment]:
    """
    Бизнес-логика получения платежа по ID.
    Возвращает None, если платеж не найден (обработку 404 оставим роутеру).
    """
    stmt = select(Payment).where(Payment.id == payment_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
