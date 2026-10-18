import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.models import Payment
from app.schemas import PaymentCreate, PaymentResponse
from app.config import settings
from app.services.payment_service import create_payment, get_payment_by_id

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        401: {"description": "Неверный или отсутствующий X-API-Key"},
        409: {"description": "Платеж с таким Idempotency-Key уже существует"},
    },
)
async def create_payment_endpoint(
    body: PaymentCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    x_api_key: str = Header(..., alias="X-API-Key"),
    session: AsyncSession = Depends(get_session),
) -> Payment:

    if x_api_key != settings.api.key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )

    payment = await create_payment(
        session=session, body=body, idempotency_key=idempotency_key
    )

    return payment


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    status_code=status.HTTP_200_OK,
)
async def get_payment(
    payment_id: uuid.UUID,
    x_api_key: str = Header(..., alias="X-API-Key"),
    session: AsyncSession = Depends(get_session),
) -> Payment:

    if x_api_key != settings.api.key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )

    payment = await get_payment_by_id(session=session, payment_id=payment_id)

    if not payment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )

    return payment
