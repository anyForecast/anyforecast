from anyforecast.executors import Executor


class TaskFuture:
    def __init__(self, task_id: str, executor: Executor):
        self.task_id = task_id
        self.executor = executor

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    @property
    def state(self):
        return self.executor.get_state()
