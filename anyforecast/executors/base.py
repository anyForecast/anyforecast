from abc import abstractmethod
from tasks.tasks import Task


class BaseExecutor:
    """Base class to inherit for concrete executors such as Celery, Ray, Local,
    etc.
    """

    @abstractmethod
    def submit(self, task: Task):
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
