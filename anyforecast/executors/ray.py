import logging

import ray

from . import base

log = logging.getLogger(__name__)


@ray.remote
def run_task(task, *args, **kwargs):
    """Runs given task."""
    task(*args, **kwargs)


class RayExecutor(base.Executor):
    def start(self):
        log.debug("Starting Ray Executor.")

    def execute(self, task, *args, **kwargs):
        self.async_ = run_task.remote(task, *args, **kwargs)
