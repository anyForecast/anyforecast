from __future__ import annotations

from typing import Any

from anyforecast.backend import BackendFuture
from anyforecast.db.models import TaskExecution


def get_future(task_id: str) -> BackendFuture:
    """Returns :class:`Future` instance from task_id.

    Parameters
    ----------
    task_id : str
        Task's UUID.
    """
    execution = TaskExecution.get_or_create(task_id=task_id)
    backend_exec = backend.BackendExecutorFactory.create(execution.backend_exec)
    future_cls = backend_exec.get_future_cls()
    return future_cls.from_id(execution.future_id)


class TaskPromise:
    """Queries task state.

    Parameters
    ----------
    task_id : str
        The task's UUID.
    """

    def __init__(
        self, task_id: str, backend_future: BackendFuture | None = None
    ):
        self.task_id = task_id
        self.backend_future = backend_future

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    def result(self) -> Any:
        """Return the result of the call that the future represents."""
        return self.backend_future.result()

    def wait(self) -> None:
        """Waits for task to complete."""
        return self.backend_future.wait()

    def done(self) -> bool:
        """Return True if the future was cancelled or finished executing."""
        return self.backend_future.done()
