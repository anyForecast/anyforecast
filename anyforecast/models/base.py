from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import Session


class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def get_or_create(cls, session: Session, **kwargs):
        """Gets object if it already exists or creates one it if it does not.

        Parameters
        ----------
        session : Session
            SQLAlchemy session.
        """
        instance = session.query(cls).filter_by(**kwargs).one_or_none()
        if instance:
            return instance
        else:
            instance = cls(**kwargs)
            session.add(instance)
            session.commit()
            return instance


Base = declarative_base(cls=Base)
