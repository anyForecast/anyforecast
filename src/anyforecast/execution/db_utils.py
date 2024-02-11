from typing import Literal

from sqlalchemy_utils.functions import create_database, database_exists

from anyforecast.db.base import Base
from anyforecast.db.engine import create_db_engine
from anyforecast.exceptions import DatabaseDoesNotExist
from anyforecast.settings import conf


def create_db() -> None:
    """Creates AnyForecast database."""
    from anyforecast.db.models import TaskExecution

    engine = create_db_engine()
    create_database(engine.url)
    Base.metadata.create_all(engine)


def check_db(if_not_exists: Literal["create", "raise"] = "raise") -> None:
    """Checks AnyForecast database.

    Parameters
    ----------
    if_not_exists : str {"create", "raise"}, default="raise"
        Behaviour if database does not exists.
    """
    db_settings = conf.get_db_settings()

    if not database_exists(db_settings.url):
        if if_not_exists == "create":
            create_db()

        elif if_not_exists == "raise":
            raise DatabaseDoesNotExist(url=db_settings.url)

        else:
            raise ValueError(
                "`if_not_exists` param can either be 'create' or 'raise', "
                f"but got '{if_not_exists}'."
            )
