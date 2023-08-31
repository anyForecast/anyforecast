from __future__ import annotations

from anyforecast.executors import ExecutorBackend

from .objects_registry import create_registry
from .task import TaskAsyncResult, TaskDescription, TaskRunner


class Executor:
    """Executes tasks on the specified executor backend."""

    def __init__(self) -> None:
        self._running_tasks = create_registry()

    def num_running_tasks(self) -> int:
        return len(self._running_tasks)

    def launch_task(
        self,
        exec_backend: ExecutorBackend,
        task_description: TaskDescription,
        **opts,
    ) -> TaskAsyncResult:
        """Launches task to executor backend.

        Internally, `launch_task` creates a TaskRunner, registers it in
        running tasks internal registry (by task id), and finally executes it
        on the executor backend.
        """
        task_runner = TaskRunner(task_description)
        self._running_tasks.put(task_description.task_id, task_runner)
        future = exec_backend.execute(task_runner, **opts)
        return TaskAsyncResult(self.task_id, future)
