import dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_dotenv(name) -> str:
    """Finds dotenv absolute paths.

    Returns
    -------
    dotenv : str
        Absolute dotenv path.
    """
    return dotenv.find_dotenv(name)


class EnvFile(BaseSettings):
    """Specifies the environment file to use."""

    env_file: str = ".env"


class AppInfo(BaseSettings):
    """Application general information."""

    name: str = "anyforecast"
    author: str = "ramonamezquita"
    email: str = "contact@anyforecast.com"


class ScriptsSettings(BaseSettings):
    """Training scripts settings."""

    repo: str = "https://github.com/anyForecast/anyforecast-scripts"
    package: str = "anyforecast_scripts"
    version: str = "main"

    model_config = SettingsConfigDict(env_prefix="SCRIPTS_")

    def create_uri(self, name: str) -> str:
        """Returns script complete uri.

        Parameters
        ----------
        name : str
            Script name.
        """
        template = "{repo}#{package}/{name}"
        return template.format(repo=self.repo, package=self.package, name=name)


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

    model_config = SettingsConfigDict(env_prefix="TOKEN_")


class DBSettings(BaseSettings):
    """Database settings.

    Parameters
    ----------
    url : str
        Database connection url.
    """

    url: str | None = None

    model_config = SettingsConfigDict(env_prefix="DB_")


class MLflowSettings(BaseSettings):
    """MLFlow settings."""

    tracking_uri: str | None = None

    model_config = SettingsConfigDict(env_prefix="MLFLOW_")


class RaySettings(BaseSettings):
    """Ray settings."""

    address: str | None = None

    model_config = SettingsConfigDict(env_prefix="RAY_")


class CelerySettings(BaseSettings):
    """Celery settings."""

    broker_url: str = "amqp://rabbitmq:5672"
    result_backend: str = "redis://redis:6379/0"
    accept_content: list[str] = ["json"]
    event_serializer: str = "json"

    model_config = SettingsConfigDict(env_prefix="CELERY_")


def get_dotenv() -> str:
    """Returns dotenv filename."""
    return find_dotenv(EnvFile().env_file)


class AnyForecastConfigParser:
    """Returns anyForecast settings.

    Use get_* methods to retrieve settings.
    Environment file is set dynamically depending on the "env_file"
    environment variable.
    """

    def __init__(self) -> None:
        self._env_file = get_dotenv()

    def get_token_settings(self) -> TokenSettings:
        """Returns token settings."""
        return TokenSettings(_env_file=self._env_file)

    def get_db_settings(self) -> DBSettings:
        """Returns database settings."""
        return DBSettings(_env_file=self._env_file)

    def get_celery_settings(self) -> CelerySettings:
        return CelerySettings(_env_file=self._env_file)

    def get_ray_settings(self) -> RaySettings:
        return RaySettings(_env_file=self._env_file)

    def get_mlflow_settings(self) -> MLflowSettings:
        return MLflowSettings(_env_file=self._env_file)

    def get_scripts_settings(self) -> ScriptsSettings:
        return ScriptsSettings(_env_file=self._env_file)


conf: AnyForecastConfigParser = AnyForecastConfigParser()

__all__ = ["conf"]
