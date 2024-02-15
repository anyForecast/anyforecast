from kombu.utils.uuid import uuid

from anyforecast import backend, backends, callback, task
from anyforecast.execution.promise import TaskPromise
from anyforecast.execution.runner import TaskRunner


class TasksExecutor:
    """Executes registered tasks on the given backend executor."""

    def list_tasks(self) -> list[str]:
        """Returns available tasks"""
        return task.factory.list()

    def get_task(self, name: str) -> task.Task:
        """Returns single task by name.

        Parameters
        ----------
        name : str
            Name of the task.
        """
        return task.factory.create(name)

    def create_task_runner(
        self,
        task: task.Task,
        args: tuple = (),
        kwargs: dict = None,
        task_id: str | None = None,
    ) -> TaskRunner:
        """Creates :class:`TaskRunner` instance."""
        task_id = task_id or uuid()
        return TaskRunner(task, args, kwargs, task_id)

    def execute(
        self,
        name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        task_id: str | None = None,
        callbacks: list[callback.Callback] = (),
        backend: backend.BackendExecutor = backends.LocalBackend(),
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
        runner = self.create_task_runner(task, args, kwargs, task_id)
        backend_promise = backend.run(runner)
        return TaskPromise(runner.task_id, backend_promise)
