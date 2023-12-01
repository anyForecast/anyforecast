from typing import Any

import ray

from . import base


@ray.remote
def run(runner: base.BackendRunner):
    """Runs given task."""
    return runner.run()


class RayFuture(base.BackendFuture):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result

    def result(self) -> Any:
        return ray.get(self.ray_async_result)

    def wait(self) -> None:
        return ray.wait([self.ray_async_result])

    def done(self) -> bool:
        return super().done()


class RayBackend(base.BackendExecutor):
    def run(self, runner: base.BackendExecutor) -> RayFuture:
        ray_async_result = run.remote(runner)
        return RayFuture(ray_async_result)
