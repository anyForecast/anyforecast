from anyforecast.app.task import TaskPromise
from anyforecast.client import AnyForecastClient


__all__ = ("execute_task",)


def execute_task(
    name,
    args: tuple = (),
    kwargs: dict = None,
    exec_backend: str | None = "local",
    task_id: str | None = None,
    **opts
):
    """Executes tasks on the specified executor backend.

    Patameters
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
    return executor.execute_task(
        name, args, kwargs, exec_backend, task_id, **opts
    )


class WebTaskExecutor:
    def __init__(self) -> None:
        self.client = AnyForecastClient()

    def execute_task(
        self,
        name,
        args: tuple = (),
        kwargs: dict = None,
        exec_backend: str | None = "local",
        task_id: str | None = None,
        **opts
    ) -> TaskPromise:
        return self.client.execute_task(
            name, args, kwargs, exec_backend, task_id, **opts
        )


executor = WebTaskExecutor()
