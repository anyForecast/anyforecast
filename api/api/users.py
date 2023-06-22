from typing import AsyncGenerator

from fastapi import Depends
from fastapi_users.db import SQLAlchemyBaseUserTableUUID, SQLAlchemyUserDatabase
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)
from sqlalchemy.orm import DeclarativeBase

from .settings import get_db_settings

DATABASE_URL = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"


def resolve_db_url(db_name) -> str:
    """Resolve database url.

    Parameters
    ----------
    db_name : str
        Database name
    """
    db_settings = get_db_settings()
    db_settings_dict = db_settings.dict()
    db_settings_dict["name"] = db_name
    return DATABASE_URL.format(**db_settings_dict)


class Base(DeclarativeBase):
    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    pass


users_db_url = resolve_db_url("users")
engine = create_async_engine(users_db_url)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def create_db_and_tables():
    """Utility function to create all the defined tables.

    This function can be called, for example, during the initialization of your
    FastAPI app.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """Returns fresh SQLAlchemy session to interact with the database.
    """
    async with async_session_maker() as session:
        yield session


async def get_user_db(session: AsyncSession = Depends(get_async_session)):
    """Returns database FastAPIUsers adapter.

    Notice that we pass to the adapter two things:
    - The session instance we just injected.
    - The User class, which is the actual SQLAlchemy model.
    """
    yield SQLAlchemyUserDatabase(session, User)
