import logging
from abc import abstractmethod
from typing import Protocol

log = logging.getLogger(__name__)


class Runner(Protocol):
    """Runner interace."""

    def run(self):
        ...


class Future:
    """Base class to inherit for concrete future/async results.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def get_state(self) -> str:
        pass


class ExecutorBackend:

    """Base class to inherit for concrete executors.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def start(self):
        """Executors may need to get things started."""

    @abstractmethod
    def execute(self, runner: Runner, **opts) -> Future:
        """Executes the task."""
        pass

    def shutdown(self):
        """Clean-up the resources associated with the Executor."""
        pass
