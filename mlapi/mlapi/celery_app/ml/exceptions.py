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

    :ivar msg: The descriptive message associated with the error.
    """
    fmt = 'An unspecified error occurred'

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs

    def __reduce__(self):
        return _exception_from_packed_args, (self.__class__, None, self.kwargs)


class BaseParquetMergerError(BaseError):
    """Base class for parquet merger validations.
    """


class UniqueGroupIdsError(BaseParquetMergerError):
    """Parquet merger datasets do not contain unique group_ids.

    That is, different group_ids exist for different parquet datasets.
    """
    fmt = 'All parquet datasets must have the same group ids.'


class GroupIdsNotFound(BaseParquetMergerError):
    """Parquet datasets do not contain group_ids.
    """
    fmt = 'Parquet datasets do not contain group_ids.'






