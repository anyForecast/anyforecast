from __future__ import annotations

from kombu.utils.uuid import uuid

from anyforecast.backend import BackendExecutor, LocalBackend
from anyforecast.callbacks import Callback
from anyforecast.tasks import Task, TasksFactory

from .promise import TaskPromise
from .runner import TaskRunner


class TasksExecutor:
    """Bridges client and task execution."""

    def __init__(self, backend_exec: BackendExecutor = LocalBackend()):
        self._backend_exec = backend_exec

    def set_backend_exec(self, backend_exec: BackendExecutor) -> None:
        self._backend_exec = backend_exec

    def get_backend_exec(self) -> BackendExecutor:
        return self._backend_exec

    def list_tasks(self) -> list[str]:
        """Returns available tasks"""
        return list(TasksFactory.registry)

    def get_task(self, name: str) -> Task:
        """Returns single task by name.

        Parameters
        ----------
        name : str
            Name of the task.
        """
        return TasksFactory.get(name)

    def execute(
        self,
        name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        task_id: str | None = None,
        callbacks: list[Callback] = (),
    ) -> TaskPromise:
        """Executes tasks on the configured backend executor.

        Parameters
        ----------
        name : str
            Name of the task to execute.

        args : tuple, default=()
            Task positional arguments.

        kwargs : dict, default=None
            Task key-word arguments.

        task_id : str, default=None
            Task identifier.

        Returns
        -------
        promise : TaskPromise
        """
        task = self.get_task(name)
        task.set_callbacks(callbacks)
        task_id = task_id or uuid()
        runner = TaskRunner(task, args, kwargs, task_id)
        future = self._backend_exec.run(runner)
        return TaskPromise(task_id=task_id, future=future)
