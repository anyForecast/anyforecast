from typing import Any

from anyforecast import backend


class TaskPromise:
    """Wrapper for backend Promises.

    Parameters
    ----------
    task_id : str
        The task's UUID.
    """

    def __init__(
        self, task_id: str, backend_promise: backend.Promise | None = None
    ):
        self.task_id = task_id
        self.backend_promise = backend_promise

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    def result(self) -> Any:
        """Return the result of the call that the future represents."""
        return self.backend_promise.result()

    def done(self) -> bool:
        """Return True if the future was cancelled or finished executing."""
        return self.backend_promise.done()
