from typing import Any

from anyforecast import task


class TaskRunner:
    """Runs task with the given parameters.

    Parameters
    ----------
    name : Task
        Task instance to run.

    args : tuple, default=()
        Task positional arguments.

    kwargs : dict, default=None
        Task key-word arguments.

    task_id : str, default=None
        Task identifier.
    """

    def __init__(
        self,
        task: task.Task,
        args: tuple = (),
        kwargs: dict = None,
        task_id: str | None = None,
    ):
        self.task = task
        self.args = args
        self.kwargs = kwargs or {}
        self.task_id = task_id

    def run(self) -> Any:
        """Runs the actual task"""
        self.notify_on_begin()

        try:
            retval = self.task(*self.args, **self.kwargs)
        except Exception as exc:
            self.notify_on_failure(exc)
            raise exc

        self.notify_on_success(retval)
        return retval

    def notify_on_begin(self) -> None:
        self.task.notify("on_begin", task_id=self.task_id)

    def notify_on_success(self, retval: Any) -> None:
        self.task.notify("on_success", retval=retval, task_id=self.task_id)

    def notify_on_failure(self, exc: Exception) -> None:
        self.task.notify("on_failure", exc=exc, task_id=self.task_id)
