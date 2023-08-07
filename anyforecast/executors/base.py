from abc import abstractmethod


class Executor:
    """Base class to inherit for concrete executors.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def start(self):
        """Executors may need to get things started."""

    @abstractmethod
    def submit(self, task, *args, **kwargs):
        """Submits the task to be executed.

        Schedules the task to be executed and returns a Future instance
        representing the execution of the task.

        Returns
        -------
        A Future representing the given call.
        """
        pass

    @abstractmethod
    def shutdown(self):
        """Clean-up the resources associated with the Executor.
        """
        pass
