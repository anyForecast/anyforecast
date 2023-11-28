from __future__ import annotations

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

    def __init__(self, task_id: str, future: BackendFuture | None = None):
        self.task_id = task_id
        self.future = future or get_future(task_id)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    @property
    def state(self):
        return self.future.get_state()
