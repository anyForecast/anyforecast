class AnyForecast:
    """AnyForecast application.

    Parameters
    ----------
    executor : anyforecast.executors.base.Executor
    """

    def __init__(self, executor: str, tasks: TaskRegistry):
        self.executor = executor
        self.tasks = tasks

    def task(self, *args, **kwargs):
        """Decorator to create a class out of any callable.
        """
