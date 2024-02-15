from typing import Any

import ray

from anyforecast import backend


@ray.remote
def run(runner: backend.Runner):
    """Runs given task."""
    return runner.run()


class RayPromise(backend.Promise):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result

    def result(self) -> Any:
        return ray.get(self.ray_async_result)

    def wait(self) -> None:
        return ray.wait([self.ray_async_result])

    def done(self) -> bool:
        return super().done()


class RayBackend(backend.BackendExecutor):
    def run(self, runner: backend.BackendExecutor) -> RayPromise:
        ray_async_result = run.remote(runner)
        return RayPromise(ray_async_result)
