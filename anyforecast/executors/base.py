import logging
from abc import abstractmethod

from anyforecast.app.task import Task

log = logging.getLogger(__name__)


class Executor:

    """Base class to inherit for concrete executors.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def start(self):
        """Executors may need to get things started."""

    @abstractmethod
    def execute(self, task: Task, *args, **kwargs) -> None:
        """Executes the task."""
        pass

    def get_state(self) -> str:
        pass

    def shutdown(self):
        """Clean-up the resources associated with the Executor."""
        pass
