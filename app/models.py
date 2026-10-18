import enum
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import JSON, DateTime, Enum, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Currency(str, enum.Enum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(str, enum.Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency_enum"), nullable=False
    )
    description: Mapped[str] = mapped_column(String(500), nullable=False)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status_enum"),
        default=PaymentStatus.PENDING,
        nullable=False,
    )
    idempotency_key: Mapped[str | None] = mapped_column(
        String(128), unique=True, nullable=True, index=True
    )
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class OutboxStatus(str, enum.Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[OutboxStatus] = mapped_column(
        Enum(OutboxStatus, name="outbox_status_enum"),
        default=OutboxStatus.PENDING,
        nullable=False,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
