from typing import Dict, Tuple, Union

from anyforecast.executors import Executor, Future, get_executor
from anyforecast.models.taskexecution import TaskExecution

from .task import Task


class TaskAsyncResult:
    def __init__(self, task_id: str, future: Future):
        self.task_id = task_id
        self.future = future

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    @property
    def state(self):
        return self.future.get_state()


class TaskRunner:
    def __init__(self, task_id: str, task: Task, args: Tuple, kwargs: Dict):
        self.task_id = task_id
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self._executor = get_executor("local")

    def run(self) -> TaskAsyncResult:
        future = self.execute_task()
        return TaskAsyncResult(self.task_id, future)

    def execute_task(self) -> None:
        return self._executor.execute(self.task, *self.args, **self.kwargs)

    def set_executor(self, executor: Union[str, Executor]) -> None:
        self._executor = get_executor(executor)

    def get_task_execution(self) -> TaskExecution:
        return TaskExecution(
            uuid=self.task_id,
            task_name=self.task.name,
            executor_class=self._executor.__class__.__name__,
        )
