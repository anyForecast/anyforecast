import ray

from . import base


@ray.remote
def execute_task(executor: base.Executor):
    """Runs given task."""
    return executor.execute


class RayFuture(base.Future):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result


class RayExecutor(base.ExecutorBackend):
    def __init__(self):
        super().__init__(future_cls=RayFuture)

    def execute(self, executor: base.Executor):
        ray_async_result = execute_task.remote(executor)
        return self.future_cls(ray_async_result)
