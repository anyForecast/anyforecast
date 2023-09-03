from __future__ import annotations

from typing import Any

from anyforecast.executors import ExecutorBackend
from anyforecast.models.taskexecution import TaskExecution

from .dbsession import Session
from .task import TaskAsyncResult, TaskContainer, TaskStatus


class TaskRunner:
    """Task runner.

    Parameters
    ----------
    executor : Executor
        Used to update task status.

    task_container : TaskContainer
        Container for the task to run.
    """

    def __init__(self, executor: Executor, task_container: TaskContainer):
        self.executor = executor
        self.task_container = task_container

        self.task_execution: TaskExecution = executor.get_execution(
            task_container.task_id
        )

    def run(self) -> Any:
        """Runs task container."""
        try:
            self.update_status(TaskStatus.RUNNING)
            retval = self.task_container.run()
        except Exception as exc:
            self.update_status(TaskStatus.FAILED)
            raise exc

        self.update_status(TaskStatus.COMPLETED)

        return retval

    def update_status(self, status: TaskStatus) -> None:
        """Updates task execution status.

        Parameters
        ----------
        status : TaskStatus
            Task status.
        """
        self.executor.update_status(self.task_execution, status)


class Executor:
    """Executes tasks on the specified executor backend."""

    def __init__(self):
        self._session = None

    def num_running_tasks(self) -> int:
        return len(self._running_tasks)

    def update_status(
        self, task_execution: TaskExecution, status: TaskStatus
    ) -> None:
        session = self.get_session()
        task_execution.status = status.value
        session.commit()

    def start_session(self) -> None:
        """Starts db session."""
        self._session = Session()

    def get_session(self) -> Session:
        """Returns db session."""
        return self._session

    def get_execution(self, task_id: str) -> TaskExecution:
        return TaskExecution.get_or_create(
            session=self.get_session(), uuid=task_id
        )

    def launch_task(
        self,
        exec_backend: ExecutorBackend,
        task_container: TaskContainer,
        **opts,
    ) -> TaskAsyncResult:
        """Launches task to executor backend.

        Internally, `launch_task` starts a fresh db session, creates a
        TaskRunner, and finally executes it on the executor backend.
        """
        self.start_session()
        task_runner = TaskRunner(self, task_container)
        future = exec_backend.execute(task_runner, **opts)
        return TaskAsyncResult(task_container.task_id, future)
