from typing import List

import dotenv
from pydantic_settings import BaseSettings


def find_dotenv(name) -> str:
    """Finds dotenv absolute paths.

    Returns
    -------
    dotenv : str
        Absolute dotenv path.
    """
    return dotenv.find_dotenv(name)


class TokenSettings(BaseSettings):
    """JWT token settings.

    Parameters
    ----------
    key : str
        Random secret key that will be used to sign the JWT tokens.

    algorithm : str, default="HS256"
        Algorithm used to sign the JWT token.

    expires : int, default=30
        Expiration of the token in minutes.
    """

    key: str
    algorithm: str = "HS256"
    expires: int = 30

    class Config:
        env_prefix = "TOKEN_"


class DBSettings(BaseSettings):
    """Database settings"""

    url: str

    class Config:
        env_prefix = "DB_"


class CelerySettings(BaseSettings):
    """Celery settings."""

    broker_url: str = "amqp://rabbitmq:5672"
    result_backend: str = "redis://redis:6379/0"
    accept_content: List[str] = ["json"]
    event_serializer: str = "json"

    class Config:
        env_prefix = "CELERY_"


class EnvSettings(BaseSettings):
    """Specifies the environment file to use."""

    env_file: str = ".env"


def get_dotenv() -> str:
    """Returns dotenv filename."""
    env_settings = EnvSettings()
    return find_dotenv(env_settings.env_file)


def get_settings(name):
    """Returns settings object.

    Environment file is set dynamically depending on the "env_file"
    environment variable.

    Parameters
    ----------
    name : str
    """
    settings = {
        "db": DBSettings,
        "token": TokenSettings,
        "celery": CelerySettings,
    }

    return settings[name](_env_file=get_dotenv())


class AnyForecastConfigParser:
    """Returns anyForecast settings."""

    def get_token_settings(self) -> TokenSettings:
        """Returns token settings."""
        return get_settings("token")

    def get_db_settings(self) -> DBSettings:
        """Returns database settings."""
        return get_settings("db")

    def get_celery_settings(self) -> CelerySettings:
        return get_settings("celery")


conf: AnyForecastConfigParser = AnyForecastConfigParser()

__all__ = ["conf"]
