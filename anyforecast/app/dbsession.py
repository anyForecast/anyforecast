from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils.functions import create_database, database_exists
from anyforecast.models.base import Base
from anyforecast.models.taskexecution import TaskExecution

# an Engine, which the Session will use for connection
# resources.
db_path = "sqlite:////Users/ramonamezquita/Projects/anyforecast/anyforecast.db"
engine = create_engine(db_path)

# a sessionmaker(), also in the same scope as the engine.
Session = sessionmaker(engine)


def create_anyforecast_database() -> None:
    create_database(engine.url)
    Base.metadata.create_all(engine)


def validate_database() -> None:
    if not database_exists(engine.url):
        create_anyforecast_database()
