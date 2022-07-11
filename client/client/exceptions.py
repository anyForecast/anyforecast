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


class PartialCredentialsError(BaseError):
    """
    Only partial credentials were found.
    :ivar cred_var: The missing credential variable name.
    """
    fmt = 'Partial credentials found in {provider}, missing: {cred_var}'


class UnknownCredentialError(BaseError):
    """Tried to insert before/after an unregistered credential type."""
    fmt = 'Credential named {name} not found.'


class UnknownServiceError(BaseError):
    fmt = 'Service named {name} not found.'


class DataNotFoundError(BaseError):
    """
    The data associated with a particular path could not be loaded.
    :ivar data_path: The data path that the user attempted to load.
    """
    fmt = 'Unable to load data for: {data_path}'


class BucketDoesNotExist(BaseError):
    """Bucket does not exist"""
    fmt = 'Bucket "{name}" does not exist. Create one using `create_bucket`.'


class FeatureError(BaseError):
    """Base exceptions for feature error"""


class UnknownFeatureTypeError(FeatureError):
    """Unknown feature type"""
    fmt = 'Unknown feature type "{feature_type}".'


class DatasetSchemaError(BaseError):
    """Base error for schema validations"""


class MissingGroupIds(DatasetSchemaError):
    """Schema does not contain group ids"""
    fmt = "Schema does not contain group ids."


class MissingDoubleUnderscore(DatasetSchemaError):
    """The schema is a multiple group ids schema but with incorrect format.
    """
    fmt = "The group ids in a multi group ids schema must contain '__'."


class DtypeNotSupported(DatasetSchemaError):
    """Feature in schema contains a different type from the supported ones.
    """
    fmt = "Feature '{name}' contains type {dtype} which is not supported. " \
          "Supported types: {supported_dtypes}"


class MissingFeatureKey(DatasetSchemaError):
    """Feature data misses needed keys.
    """
    fmt = "Key '{key}' is missing for feature with position {position}."


class TypesMismatch(DatasetSchemaError):
    """Dataframe dtype does not match schema type.
    """
    fmt = "Schema dtype '{schema_dtype}' does not match pandas " \
          "dtype '{pandas_dtype}' for feature '{name}'."


class DatasetTypeNotKnown(DatasetSchemaError):
    """Dataset type not known.
    """
    fmt = "Dataset type '{type}' not known. Available types: {available}"


class DistinctNumberOfFeatures(DatasetSchemaError):
    """Schema and dataframe have different number of features
    """
    fmt = "Expected {n_schema} number of features in dataframe, but found " \
          "{n_df}"


class MissingTimestamp(DatasetSchemaError):
    """Schema does not contain timestamp type.
    """
    fmt = "Schema does not contain the 'timestamp' feature."


class DuplicatedFeature(DatasetSchemaError):
    """Schema contains duplicated feature.
    """
    fmt = "Schema contains duplicated feature '{name}'."


class TargetDatasetError(DatasetSchemaError):
    """Base class for Target dataset errors
    """


class ExtraFeaturesInPandasError(DatasetSchemaError):
    """Dataset contains different features than schema.
    """
    fmt = "The following features inside pandas `DataFrame` are not part " \
          "of the `schema` and must be either removed from the `DataFrame` " \
          "or added to the `schema`: {extras}. "


class ExtraFeaturesInSchemaError(DatasetSchemaError):
    """Schema contains different features than dataset.
    """
    fmt = "The following features inside `schema` are not part of " \
          "the `DataFrame` and must be either removed from the `schema` or " \
          "added to the `DataFrame`: {extras}. "


class MissingTarget(TargetDatasetError):
    """Schema for Target dataset does not contain a target feature.
    """
    fmt = "Schema for dataset_type='target' requires a 'target' feature of " \
          "type 'float'"


class BaseEndpointResolverError(BaseError):
    """Base error for endpoint resolving errors.
    Should never be raised directly, but clients can catch
    this exception if they want to generically handle any errors
    during the endpoint resolution process.
    """


class NoRegionError(BaseEndpointResolverError):
    """No region/endpoint_name was specified."""
    fmt = 'You must specify a region/endpoint name.'


class InvalidEndpointForService(BaseEndpointResolverError):
    """Endpoint does not exist for the selected service."""
    fmt = 'Endpoint {endpoint} does not exist for selected service {service}.'
