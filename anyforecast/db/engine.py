from sqlalchemy import create_engine

from anyforecast.settings import conf


def create_db_engine():
    """Creates database connection engine."""
    db_settings = conf.get_db_settings()
    return create_engine(db_settings.url)
