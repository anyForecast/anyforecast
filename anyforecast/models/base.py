from typing import Any, Literal

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy_utils.functions import create_database, database_exists

from anyforecast.exceptions import DatabaseDoesNotExist

from .engine import create_db_engine, db_settings

sessionfactory = sessionmaker(create_db_engine())


class _Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def get_or_create(cls, session: Session | None = None, **kwargs):
        """Gets object if it already exists or creates one it if it does not.

        Parameters
        ----------
        session : Session
            SQLAlchemy session.
        """
        if session is None:
            session = sessionfactory()

        instance = session.query(cls).filter_by(**kwargs).one_or_none()
        if instance:
            return instance
        else:
            instance = cls(**kwargs)
            session.add(instance)
            session.commit()
            return instance


Base = declarative_base(cls=_Base)


def create_anyforecast_database() -> None:
    """Creates AnyForecast database."""
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
    if not database_exists(db_settings.url):
        if if_not_exists == "create":
            create_anyforecast_database()

        elif if_not_exists == "raise":
            raise DatabaseDoesNotExist(url=db_settings.url)

        else:
            raise ValueError(
                "`if_not_exists` param can either be 'create' or 'raise', "
                f"but got '{if_not_exists}'."
            )
