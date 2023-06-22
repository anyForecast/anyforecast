import os
from typing import Literal

import dotenv
from pydantic import BaseSettings

ENVS_DIR = "api/envs/"


def find_dotenv(name) -> str:
    """Finds dotenv absolute paths.

    Returns
    -------
    dotenv : str
        Absolute dotenv path.
    """
    filename = os.path.join(ENVS_DIR, name)
    return dotenv.find_dotenv(filename)


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
        env_file = find_dotenv(".env.token")
        env_prefix = "TOKEN_"


class DBSettings(BaseSettings):
    """Database settings

    Parameters
    ----------
    host: str
        Database host.

    user : str
        Database user.

    password : str
        Database password.

    port : int
        Database port.
    """
    host: str
    user: str
    password: str
    port: int = 5432

    class Config:
        env_prefix = "DB_"


class APISettings(BaseSettings):
    """API settings.

    Holds api general information.

    Parameters
    ----------
    name : str
        Application name.

    author : str
        Application author.

    email : str
        Application contact email.
    """
    name: str = "anyForecast"
    author: str = "anyForecast"
    email: str = "concat@anyforecast.com"


class EnvSettings(BaseSettings):
    env: Literal["local", "staging", "production"] = "local"

    class Config:
        env_file = find_dotenv(".env")


def get_api_settings() -> APISettings:
    """Returns api settings.

    Api settings contains general and public information about the application.
    """
    return APISettings()


def get_token_settings() -> TokenSettings:
    """Returns token settings.
    """
    return TokenSettings()


def get_db_settings() -> DBSettings:
    """Returns database settings.
    """
    return get_settings("db")


def get_settings(name: Literal["db", "token"]):
    """Returns settings object.

    Environment file is set dynamically depending on the "env" environment
    variable.

    Parameters
    ----------
    name : str, {"api", "db", "token"}

    """
    settings = {
        'db': DBSettings
    }

    env_settings = EnvSettings()
    return settings[name](_env_file=find_dotenv(env_settings.env))
