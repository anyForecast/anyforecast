from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Any, Dict, Tuple

from kombu.utils.uuid import uuid

from anyforecast import backends, tasks
from anyforecast.db.base import sessionfactory
from anyforecast.db.taskexecution import TaskExecution
from anyforecast.exceptions import RunningTasksDoesNotExist

from .db_utils import check_db
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

    task: tasks.Task
    args: Tuple
    kwargs: Dict
    task_id: str

    def run(self) -> Any:
        """Runs the actual task"""
        return self.task(*self.args, **self.kwargs)


class RunningTasks(dict):
    """Map of running task promises."""

    def __missing__(self, key):
        raise RunningTasksDoesNotExist(task_id=key)

    def add(self, promise: TaskPromise):
        self[promise.task_id] = promise

    def remove(self, task_id: str):
        self.pop(task_id)

    def get(self, task_id: str):
        return self[task_id]


class TaskExecutor:
    """Executes tasks on the selected backend.

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
        backend_exec: backends.BackendExecutor,
    ):
        self.task_container = task_container
        self.backend_exec = backend_exec
        check_db()

    def submit(self, **opts) -> backends.BackendFuture:
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

    def create_backend_exec(
        self, name: str, **opts
    ) -> backends.BackendExecutor:
        """Creates backend executor.

        Parameters
        ----------
        name : str
            Backend executor name.
        """
        return backends.BackendExecutorFactory.create(name, **opts)

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
        backend_exec = self.create_backend_exec(backend_exec, **opts)
        executor = TaskExecutor(task_container, backend_exec)
        future = executor.submit(**opts)
        return TaskPromise(task_container.task_id, future)


class RegisteredTasksExecutor:
    """Provides access to the execution of registered tasks.

    Call :meth:`list_tasks` to see available tasks.
    """

    def __init__(self):
        check_db(if_not_exists="create")
        self._running = RunningTasks()

    def save_promise(self, promise: TaskPromise) -> None:
        """Saves promise."""
        self._running.add(promise)

    def get_promise(self, task_id: str) -> TaskPromise:
        return self._running.get(task_id)

    def execute_async(
        self,
        name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        backend_exec: str = "local",
        task_id: str = None,
        **opts,
    ) -> TaskPromise:
        """Executes registered tasks on the specified executor backend.

        Patameters
        ----------
        name : str
            Name of the task name to execute.

        args : tuple, default=()
            Task positional arguments.

        kwargs : dict, default=None
            Task key-word arguments.

        backend : str or ExecutorBackend, default="local"
            Backend executor.

        task_id : str, default=None
            Task identifier.

        **opts : optional args
            Optional arguments to executor backend.
        """
        if kwargs is None:
            kwargs = {}

        client_executor_bridge = ClientExecutorBridge()
        task = self.get_task(name)
        task_id = task_id or uuid()
        task_container = TaskContainer(task, args, kwargs, task_id)
        promise = client_executor_bridge.submit_task(
            backend_exec, task_container, **opts
        )
        self.save_promise(promise)
        return promise

    def execute(
        self,
        name: str,
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> Any:
        """Executes registered tasks synchronously.

        Patameters
        ----------
        name : str
            Name of the task name to execute.

        args : tuple, default=()
            Task positional arguments.

        kwargs : dict, default=None
            Task key-word arguments.
        """
        if kwargs is None:
            kwargs = {}

        return self.get_task(name)(*args, **kwargs)

    def list_tasks(self) -> list[str]:
        """Returns available tasks"""
        return list(tasks.TasksFactory.registry)

    def get_task(self, name: str) -> tasks.Task:
        """Returns single task by name.

        Parameters
        ----------
        name : str
            Name of the task.
        """
        return tasks.TasksFactory.get(name)
