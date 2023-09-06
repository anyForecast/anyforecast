from __future__ import annotations

from datetime import datetime
from functools import cached_property
from typing import Any

from anyforecast.executors import ExecutorBackend, Future
from anyforecast.models.dbsession import DBSession
from anyforecast.models.taskexecution import TaskExecution

from .task import TaskAsyncResult, TaskContainer, TaskStatus


class TaskRunner:
    def __init__(self, executor: Executor, task_container: TaskContainer):
        self.executor = executor
        self.task_container = task_container

    @cached_property
    def execution(self) -> TaskExecution:
        """Returns the TaskExecution object associated to this run."""
        task_id = self.task_container.task_id
        task_name = self.task_container.task.name
        return self.executor.get_execution(task_id, task_name=task_name)

    def run(self) -> Any:
        """Run the actual task and measure its runtime."""
        try:
            self.start()
            retval = self.task_container.run()
        except Exception as exc:
            self.finish(TaskStatus.FAILED)
            raise exc

        self.finish(TaskStatus.COMPLETED)
        return retval

    def start(self) -> None:
        """Updates initial task execution attributes."""
        self.start_time = datetime.now()
        self.update_execution("start_time", self.start_time)
        self.update_status(TaskStatus.RUNNING)

    def finish(self, status: TaskStatus) -> None:
        """Updates final task execution attributes.

        Parameters
        ----------
        status : TaskStatus
            Task final status.
        """
        self.end_time = datetime.now()
        minutes = (self.end_time - self.start_time).total_seconds() / 60
        self.update_execution("end_time", self.end_time)
        self.update_execution("duration", minutes)
        self.update_status(status)

    def update_execution(self, attr: str, value: Any) -> None:
        """Updates the task execution object associated to this run.

        Parameters
        ----------
        attr : str
            Attribute to update.

        value : Any
            Value to set.
        """
        self.executor.update_execution(self.execution, attr, value)

    def update_status(self, status: TaskStatus) -> None:
        """Updates task execution status.

        Parameters
        ----------
        status : TaskStatus
            Status to set.
        """
        self.executor.update_status(self.execution, status)


class Executor:
    """Executes tasks on the specified executor backend."""

    def __init__(self):
        self.db_session = DBSession()

    def get_execution(self, task_id: str, **kwargs) -> TaskExecution:
        """Retrieves task execution from database.

        Parameters
        ----------
        task_id : str
            Task's UUID.
        """
        return self.db_session.get_or_create(
            TaskExecution, task_id=task_id, **kwargs
        )

    def update_execution(
        self, task_execution: TaskExecution, attr: str, value: Any
    ) -> None:
        """Updates task execution object and commits the result.

        Parameters
        ----------
        task_execution : TaskExecution
            Task execution object to update.

        attr : str
            Attribute to update.

        value : Any
            Value to set.
        """
        if not isinstance(task_execution, TaskExecution):
            raise TypeError(
                "Object must be of type TaskExecution. "
                f"Instead got: {type(task_execution).__name__}"
            )

        self.db_session.update(task_execution, attr, value)

    def update_status(
        self, task_execution: TaskExecution, status: TaskStatus
    ) -> None:
        """Updates task execution status.

        Parameters
        ----------
        task_execution : TaskExecution
            Task execution object to update.

        status : TaskStatus
            Status to set.
        """
        self.update_execution(task_execution, "status", status.value)

    def launch_task(
        self,
        exec_backend: ExecutorBackend,
        task_container: TaskContainer,
        **opts,
    ) -> TaskAsyncResult:
        """Launches task to executor backend.

        Parameters
        ----------
        exec_backend : ExecutorBackend
            Executor backend where the task will be run.

        task_container : TaskContainer
            Task container.

        **opts : keyword arguments.
            Optional keyword argument to pass to executor backend.
        """
        task_runner = TaskRunner(self, task_container)
        execution: TaskExecution = task_runner.execution
        self.update_status(execution, TaskStatus.READY)
        future: Future = exec_backend.execute(task_runner, **opts)
        self.update_execution(execution, "future_id", future)
        return TaskAsyncResult(task_container.task_id, future)
