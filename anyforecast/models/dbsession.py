from typing import Any, Type

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils.functions import create_database, database_exists

from anyforecast.models.base import Base
from anyforecast.settings import conf

db_settings = conf.get_db_settings()
engine = create_engine(db_settings.url)
Session = sessionmaker(engine)


def create_anyforecast_database() -> None:
    create_database(engine.url)
    Base.metadata.create_all(engine)


def validate_database() -> None:
    if not database_exists(engine.url):
        create_anyforecast_database()


class DBSession:
    """Wrapper for SQLAlchemy database session.

    Provides commonly used routines as class methods.
    """

    def __init__(self):
        self._session = Session()

    def get_session(self) -> Session:
        return self._session

    def update(self, entry: Base, attr: str, value: Any) -> None:
        """Updates db entry and commits result.

        Parameters
        ----------
        entry : Base
            Database entry to update.

        attr : str
            Attribute to update.

        value : Any
            Value to set.
        """
        setattr(entry, attr, value)
        self.get_session().commit()

    def get_or_create(self, table: Type[Base], **kwargs) -> Any:
        """Gets object if it already exists or creates one it if it does not.

        Equivalent to Django's get_or_create.
        """
        return table.get_or_create(self.get_session(), **kwargs)
