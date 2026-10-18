import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, HttpUrl

from app.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=2)
    currency: Currency
    description: str = Field(..., min_length=1, max_length=500)
    metadata: dict[str, Any] | None = None
    webhook_url: HttpUrl


class PaymentResponse(BaseModel):
    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] | None = Field(default=None, alias="metadata_")
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True, "populate_by_name": True}
