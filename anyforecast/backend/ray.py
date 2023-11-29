import ray

from anyforecast.settings import conf

from . import base

ray_settings = conf.get_ray_settings()
ray.init(address=ray_settings.address)


@ray.remote
def run(runner: base.BackendRunner):
    """Runs given task."""
    return runner.run()


class RayFuture(base.BackendFuture):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result


class RayExecutor(base.BackendExecutor):
    def __init__(self):
        super().__init__(future_cls=RayFuture)

    def execute(self, runner: base.BackendExecutor) -> RayFuture:
        ray_async_result = run.remote(runner)
        return RayFuture(ray_async_result)
