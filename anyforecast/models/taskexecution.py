import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, JSON

from .base import Base


class TaskExecution(Base):
    """Stores the task execution information."""

    __tablename__ = "task_execution"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(36))
    task_name = Column(String(100))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    duration = Column(Float)
    status = Column(Integer)
    future_id = Column(String(36))
    hostname = Column(String(500))
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)
