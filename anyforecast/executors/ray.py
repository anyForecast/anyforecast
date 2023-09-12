import logging

import ray

from . import base


log = logging.getLogger(__name__)


@ray.remote
def run_task(task, *args, **kwargs):
    """Runs given task."""
    task(*args, **kwargs)


class RayFuture(base.Future):
    def __init__(self, ray_async_result):
        self.ray_async_result = ray_async_result


class RayExecutor(base.ExecutorBackend):
    def start(self):
        log.debug("Starting Ray Executor.")

    def execute(self, task, *args, **kwargs):
        ray_async_result = run_task.remote(task, *args, **kwargs)
        return RayFuture(ray_async_result)