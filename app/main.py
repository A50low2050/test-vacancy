import uvicorn
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.payments import router as payments_router
from app.config import settings
from app.rabbitmq import rabbitmq_client


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("App is launch")

    try:
        await rabbitmq_client.connect()
        print("✅ Connected to RabbitMQ")
    except Exception as e:
        print(f"⚠️  Failed to connect to RabbitMQ: {e}")
        print("⚠️  Payment events will not be published")

    yield

    print("Shutting down...")
    await rabbitmq_client.close()


app = FastAPI(
    title="Payment Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(payments_router, prefix="/api/v1")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":

    uvicorn.run(
        "app.main:app",
        host=settings.uvicorn.host,
        port=settings.uvicorn.port,
        reload=settings.uvicorn.reload,
    )
