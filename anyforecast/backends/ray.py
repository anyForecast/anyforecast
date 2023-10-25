import ray

from . import base


@ray.remote
def execute_task(executor: base.Executor):
    """Runs given task."""
    return executor.execute


class RayFuture(base.BackendFuture):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result


@base.BackendExecutorFactory.register("ray")
class RayExecutor(base.BackendExecutor):
    def __init__(self):
        super().__init__(future_cls=RayFuture)

    def execute(self, executor: base.Executor):
        ray_async_result = execute_task.remote(executor)
        return self.future_cls(ray_async_result)
