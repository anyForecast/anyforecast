def _exception_from_packed_args(exception_cls, args=None, kwargs=None):
    # This is helpful for reducing Exceptions that only accept kwargs as
    # only positional arguments can be provided for __reduce__
    # Ideally, this would also be a class method on the BotoCoreError
    # but instance methods cannot be pickled.
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    return exception_cls(*args, **kwargs)


class BaseError(Exception):
    """The base exception class for errors.
    """
    fmt = 'An unspecified error occurred'

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs

    def __reduce__(self):
        return _exception_from_packed_args, (self.__class__, None, self.kwargs)


class DataNotFoundError(BaseError):
    """The data associated with a particular path could not be loaded.
    """
    fmt = 'Unable to load data for: {data_path}'


class UnknownServiceError(BaseError):
    fmt = 'Service named "{name}" not found.'


class UnknownAlgorithmError(BaseError):
    fmt = "Algorithm {algorithm} does not exist. "


class UnknownSchemaKeyError(BaseError):
    fmt = 'Schema key "{key}" does not exist.'


class UnknownPandasSerializer(BaseError):
    fmt = 'Pandas serializer "{name}" does not exist.'


class UnknownTaskError(BaseError):
    fmt = 'Task with name "{name}" does not exist in the registry.'
