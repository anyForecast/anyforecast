import ray

from . import base


@ray.remote
def run_task(runner: base.BackendRunner):
    """Runs given task."""
    return runner.run()


class RayFuture(base.BackendFuture):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result


class RayBackend(base.Backend):
    def run(self, runner: base.BackendRunner) -> base.BackendFuture:
        ray_async_result = run_task.remote(runner)
        return self.future_cls(ray_async_result)
