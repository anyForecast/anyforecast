from datetime import datetime
from enum import Enum
from typing import Any

from anyforecast.callback import Callback
from anyforecast.db.base import sessionfactory
from anyforecast.db.models import TaskExecution


class TaskStatus(Enum):
    """Task status.

    Attributes
    ----------
    PENDING. Task has not been run.
    RUNNING. Task is currently in progress.
    COMPLETED. Task has completed succesfully.
    FAILED. An error occurred with the task.
    """

    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class DatabaseCallback(Callback):
    def get_or_create_execution(self, task_id) -> TaskExecution:
        return TaskExecution.get_or_create(self.session, task_id=task_id)

    def on_begin(self, task_id: str) -> None:
        # Make new child process use its own database session.
        self.session = sessionfactory()

        self.start_time = datetime.now()
        execution = self.get_or_create_execution(task_id)
        execution.start_time = self.start_time
        execution.status = TaskStatus.RUNNING.value
        self.session.commit()

    def on_failure(self, exc: Any, task_id: str) -> None:
        self.finish(task_id, TaskStatus.FAILED)

    def on_success(self, retval: Any, task_id: str) -> None:
        self.finish(task_id, TaskStatus.COMPLETED)

    def finish(self, task_id: str, status: TaskStatus) -> None:
        """Updates final task execution attributes.

        Parameters
        ----------
        status : TaskStatus
            Task final status.
        """
        end_time = datetime.now()
        execution = self.get_or_create_execution(task_id)

        minutes = (end_time - self.start_time).total_seconds() / 60
        execution.end_time = end_time
        execution.duration = minutes
        execution.status = status.value
        self.session.commit()
