from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Dict, Tuple

from anyforecast.executors import (
    BackendExecutor,
    BackendExecutorFactory,
    Future,
)
from anyforecast.models.base import sessionfactory
from anyforecast.models.taskexecution import TaskExecution
from anyforecast.models.utils import check_db
from anyforecast.tasks import Task

from .promise import TaskPromise


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


@dataclass
class TaskContainer:
    """Holds the Task instance and its parameters.

    Parameters
    ----------
    task : Task
        Task instance.

    args : tuple
            Task positional arguments.

    kwargs : dict
        Task key-word arguments

    task_id : str
        The task's UUID.
    """

    task: Task
    args: Tuple
    kwargs: Dict
    task_id: str

    def run(self) -> Any:
        """Runs the actual task"""
        return self.task(*self.args, **self.kwargs)


class TaskExecutor:
    """Executes the actual task.

    :class:`TaskExecutor` instances encapulsate the running of a single task.
    Each running process starts a fresh db session used to update the state of
    the task and its runtime values.

    Parameters
    ----------
    task_container : TaskContainer
        Holds the Task instance and its parameters.

    task_execution : TaskExecution
        SQLAlchemy TaskExecution model instance.

    exec_backend : ExecutorBackend
        Executor backend.
    """

    def __init__(
        self,
        task_container: TaskContainer,
        backend_exec: BackendExecutor,
    ):
        self.task_container = task_container
        self.backend_exec = backend_exec
        check_db()

    def submit(self, **opts) -> Future:
        return self.backend_exec.execute(self, **opts)

    def execute(self) -> Any:
        # Make new child process use its own databse session.
        self.session = sessionfactory()

        try:
            self.start()
            retval = self.task_container.run()
        except Exception as exc:
            self.finish(TaskStatus.FAILED)
            raise exc

        self.finish(TaskStatus.COMPLETED)
        return retval

    @cached_property
    def execution(self) -> TaskExecution:
        """Returns the TaskExecution object associated to this run."""
        task_id = self.task_container.task_id
        return TaskExecution.get_or_create(self.session, task_id=task_id)

    def start(self) -> None:
        """Updates initial task execution attributes."""
        self.start_time = datetime.now()
        self.update_execution("start_time", self.start_time)
        self.update_execution("task_name", self.task_container.task.name)
        self.update_status(TaskStatus.RUNNING)

    def finish(self, status: TaskStatus) -> None:
        """Updates final task execution attributes.

        Parameters
        ----------
        status : TaskStatus
            Task final status.
        """
        self.end_time = datetime.now()
        minutes = (self.end_time - self.start_time).total_seconds() / 60
        self.update_execution("end_time", self.end_time)
        self.update_execution("duration", minutes)
        self.update_status(status)

    def update_execution(self, attr: str, value: Any) -> None:
        """Updates the task execution object associated to this run.

        Parameters
        ----------
        attr : str
            Attribute to update.

        value : Any
            Value to set.
        """
        setattr(self.execution, attr, value)
        self.session.commit()

    def update_status(self, status: TaskStatus) -> None:
        """Updates task execution status.

        Parameters
        ----------
        status : TaskStatus
            Status to set.
        """
        self.update_execution("status", status.value)


class ClientExecutorBridge:
    """Bridges client and task execution."""

    def create_task_execution(self, task_id: str, backend_exec: str) -> None:
        """Create TaskExecution entry in database with PENDING status.

        Parameters
        ----------
        task_id : str
            Task's UUID

        backend_exec : str
            Backend executor name.
        """
        TaskExecution.get_or_create(
            task_id=task_id,
            status=TaskStatus.PENDING.value,
            backend_exec=backend_exec,
        )

    def submit_task(
        self,
        backend_exec: str,
        task_container: TaskContainer,
        **opts,
    ) -> TaskPromise:
        """Submits task to executor.

        Parameters
        ----------
        backend_exec : str
            Backend executor name.

        task_container : TaskContainer
            Task container.

        **opts : keyword arguments.
            Optional keyword arguments to pass to executor backend.
        """
        self.create_task_execution(task_container.task_id, backend_exec)
        backend_exec = BackendExecutorFactory.create(backend_exec, **opts)
        task_executor = TaskExecutor(task_container, backend_exec)
        future = task_executor.submit()
        return TaskPromise(task_container.task_id, future)
