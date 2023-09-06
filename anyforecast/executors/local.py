import logging

from . import base

log = logging.getLogger(__name__)


class LocalExecutor(base.ExecutorBackend):
    def execute(self, runner: base.Runner, **opts):
        return runner.run()

    def shutdown(self):
        pass
