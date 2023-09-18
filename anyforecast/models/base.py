from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from .engine import create_db_engine

engine = create_db_engine()
sessionfactory = sessionmaker(engine)


class Base:
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


Base = declarative_base(cls=Base)
