from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).parent.parent


class UvicornSettings(BaseModel):
    host: str
    port: int
    reload: bool


class SqliteSettings(BaseModel):
    """Настройки SQLite для локальной разработки"""

    url: str
    url_async: str


class PostgresSettings(BaseModel):
    """Настройки PostgreSQL"""

    host: str
    port: int
    user: str
    password: str
    db: str

    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

    @property
    def url_sync(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class RabbitMQSettings(BaseModel):
    """Настройки Rabbit для локальной разработки"""

    host: str
    port: int
    user: str
    password: str
    exchange: str
    queue: str

    @property
    def url(self) -> str:
        return f"amqp://{self.user}:{self.password}" f"@{self.host}:{self.port}/"


class APISettings(BaseModel):
    """Настройки API и аутентификации"""

    key: str


class Settings(BaseSettings):

    uvicorn: UvicornSettings
    sqlite: SqliteSettings
    postgres: PostgresSettings
    rabbitmq: RabbitMQSettings
    api: APISettings

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )


settings = Settings()  # type: ignore[call-arg]
