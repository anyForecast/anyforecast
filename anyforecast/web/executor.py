from anyforecast.execution import RegisteredTasksExecutor, TaskPromise
from typing import Any

__all__ = ("execute_task",)


def execute_async(
    name,
    args: tuple = (),
    kwargs: dict = None,
    exec_backend: str | None = "local",
    task_id: str | None = None,
    **opts
):
    """Executes tasks on the specified executor backend.

    Parameters
    ----------
    name : str
        Name of the task name to execute.

    args : tuple, default=()
        Task positional arguments.

    kwargs : dict, default=None
        Task key-word arguments

    exec_backend : str or ExecutorBackend, default="local"
        Executor backend.

    task_id : str, default=None
        Task identifier.

    **opts : optional args
        Optional arguments to executor backend.
    """
    return executor.execute_async(
        name, args, kwargs, exec_backend, task_id, **opts
    )


def execute(name, args: tuple = (), kwargs: dict = None):
    """Executes tasks on the specified executor backend.

    Parameters
    ----------
    name : str
        Name of the task name to execute.

    args : tuple, default=()
        Task positional arguments.

    kwargs : dict, default=None
        Task key-word arguments
    """
    return executor.execute(name, args, kwargs)


class WebTaskExecutor:
    def __init__(self) -> None:
        self.executor = RegisteredTasksExecutor()

    def execute_async(
        self,
        name,
        args: tuple = (),
        kwargs: dict = None,
        exec_backend: str | None = "local",
        task_id: str | None = None,
        **opts
    ) -> TaskPromise:
        return self.executor.execute_async(
            name, args, kwargs, exec_backend, task_id, **opts
        )

    def execute(self, name, args: tuple = (), kwargs: dict = None) -> Any:
        return self.executor.execute(name, args, kwargs)


executor = WebTaskExecutor()
