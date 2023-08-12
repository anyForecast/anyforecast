from typing import Dict, Tuple, Union

from anyforecast.app.taskfuture import TaskFuture
from anyforecast.executors import Executor, LocalExecutor, get_executor
from anyforecast.models.taskexecution import TaskExecution

from .task import Task


class TaskRunner:
    def __init__(self, task_id: str, task: Task, args: Tuple, kwargs: Dict):
        self.task_id = task_id
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self._executor = LocalExecutor()

    def run(self) -> TaskFuture:
        self.execute_task()
        return TaskFuture(self.task_id, self._executor)

    def execute_task(self) -> None:
        self._executor.execute(self.task, *self.args, **self.kwargs)

    def set_executor(self, executor: Union[str, Executor]) -> None:
        self._executor = get_executor(executor)

    def get_task_execution(self) -> TaskExecution:
        return TaskExecution(
            uuid=self.task_id,
            task_name=self.task.name,
            executor_class=self._executor.__class__.__name__,
        )
