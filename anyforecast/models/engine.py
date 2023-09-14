from sqlalchemy import Engine, create_engine

from anyforecast.settings import conf

db_settings = conf.get_db_settings()


def create_db_engine() -> Engine:
    """Creates database connection engine."""
    return create_engine(db_settings.url)
