from __future__ import annotations

from kombu.utils.uuid import uuid

from anyforecast import backend, callbacks, tasks
from anyforecast.execution import promise, runner


class TasksExecutor:
    """Bridges client and task execution."""

    def __init__(
        self, backend_exec: backend.BackendExecutor = backend.LocalBackend()
    ):
        self._backend_exec = backend_exec

    def set_backend_exec(self, backend_exec: backend.BackendExecutor) -> None:
        backend.check_backend_exec(backend_exec)
        self._backend_exec = backend_exec

    def get_backend_exec(self) -> backend.BackendExecutor:
        return self._backend_exec

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

    def create_task_runner(
        self,
        task: tasks.Task,
        args: tuple = (),
        kwargs: dict = None,
        task_id: str | None = None,
    ) -> runner.TaskRunner:
        """Creates :class:`TaskRunner` instance."""
        task_id = task_id or uuid()
        return runner.TaskRunner(task, args, kwargs, task_id)

    def start_backend_exec(self) -> None:
        """Stars backend executor."""
        self._backend_exec.start()

    def execute(
        self,
        name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        task_id: str | None = None,
        callbacks: list[callbacks.Callback] = (),
    ) -> promise.TaskPromise:
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
        runner = self.create_task_runner(task, args, kwargs, task_id)
        backend_future = self._backend_exec.run(runner)
        return promise.TaskPromise(runner.task_id, backend_future)
