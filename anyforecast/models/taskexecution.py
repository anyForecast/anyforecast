import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String

from .base import Base


class TaskExecution(Base):
    """Stores the task execution information."""

    __tablename__ = "task_execution"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(36))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(Float)
    state = Column(String(20))
    task_name = Column(String(100))
    executor_class = Column(String(500))
    hostname = Column(String(500))
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)
    exit_code = Column(Integer)
    exit_message = Column(String(2500))
    error_message = Column(String(2500))
