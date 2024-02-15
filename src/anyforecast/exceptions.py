def _exception_from_packed_args(exception_cls, args=None, kwargs=None):
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    return exception_cls(*args, **kwargs)


class BaseError(Exception):
    """The base exception class for errors."""

    fmt = "An unspecified error occurred"

    def __init__(self, *args, **kwargs):
        msg = self.fmt.format(*self.args, **kwargs)
        Exception.__init__(self, msg)
        self.args = args
        self.kwargs = kwargs

    def __reduce__(self):
        return _exception_from_packed_args, (
            self.__class__,
            self.args,
            self.kwargs,
        )


class TaskNotRegistered(BaseError):
    fmt = 'Task with name "{0}" is not registered.'


class InvalidTaskError(BaseError):
    fmt = 'Task class "{name}" must specify .name attribute.'


class DatabaseDoesNotExist(BaseError):
    fmt = "anyforecast database does not exist in the given url: {url}"


class RunningTasksDoesNotExist(BaseError):
    fmt = "Running task with id '{task_id}' was not found."


class ExecutorBackendDoesNotExist(BaseError):
    fmt = "Executor backend '{name}' does not exist. Available: {available}."
