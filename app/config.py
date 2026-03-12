import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BotConfig:
    token: str


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str = "prefer"


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required.")
    return value


def get_bot_config() -> BotConfig:
    return BotConfig(token=_require_env("TELEGRAM_BOT_TOKEN"))


def get_database_config() -> DatabaseConfig:
    port_raw = os.getenv("DB_PORT", "5432").strip()

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError("DB_PORT must be an integer.") from exc

    return DatabaseConfig(
        host=_require_env("DB_HOST"),
        port=port,
        database=_require_env("DB_NAME"),
        user=_require_env("DB_USER"),
        password=_require_env("DB_PASSWORD"),
        sslmode=os.getenv("DB_SSLMODE", "prefer").strip() or "prefer",
    )
