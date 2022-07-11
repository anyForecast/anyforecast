from abc import abstractmethod, ABCMeta

import pyarrow as pa

from client.exceptions import (
    TypesMismatch,
    ExtraFeaturesInPandasError,
    ExtraFeaturesInSchemaError
)
from ..schema import SchemaResolver


def make_dataset(data, schema):
    schema_resolver = SchemaResolver(schema)
    return Dataset(data, schema_resolver)


class DatasetValidator(metaclass=ABCMeta):
    def __init__(self, df, schema_resolver):
        self.df = df
        self.schema_resolver = schema_resolver

    @abstractmethod
    def validate(self):
        pass


class DtypesValidator(DatasetValidator):
    _DTYPES_TO_PANDAS = {
        'float': ['float64', 'float32'],
        'int': ['int64', 'int32'],
        'string': ['object'],
        'timestamp': ['datetime64[ns]']
    }

    def __init__(self, df, schema_resolver):
        super().__init__(df, schema_resolver)

    def validate(self):
        names_to_dtype = self.schema_resolver.get_names_to_dtype()
        for name, dtype in names_to_dtype.items():
            self._validate_dtype(name, dtype)

    def _validate_dtype(self, name, dtype):
        """Validates df dtypes coincide schema types.
        """
        pandas_dtype = self._get_pandas_dtype(name)
        if pandas_dtype not in self._DTYPES_TO_PANDAS[dtype]:
            raise TypesMismatch(name=name,
                                schema_dtype=dtype,
                                pandas_dtype=pandas_dtype)

    def _get_pandas_dtype(self, name):
        return self.df.dtypes[name].name


class NamesValidator(DatasetValidator):
    def __init__(self, df, schema_resolver):
        super().__init__(df, schema_resolver)

    def validate(self):
        names_set = set(self.schema_resolver.get_names())
        pandas_set = set(self.df)

        if not names_set.issubset(pandas_set):
            extras = names_set.difference(pandas_set)
            raise ExtraFeaturesInSchemaError(extras=extras)

        if not pandas_set.issubset(names_set):
            extras = pandas_set.difference(names_set)
            raise ExtraFeaturesInPandasError(extras=extras)


class Dataset(metaclass=ABCMeta):
    """Base class for datasets. Collects validations common for all derived
    Datasets.

    A dataset consists on a dataframe and its schema.

    Notes
    -----
    Resume of validations at __init__:
     - A = B where A and B are the set of features name define in the
        schema and the names of the pandas dataframe columns, respectively.

    - Dtypes from pandas dataframe match the ones defined in the schema.

    Parameters
    ----------
    df : pd.DataFrame
        Data to write.

    schema_resolver : client.schema.SchemaResolver
        :class:`SchemaResolver` object.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """
    _DTYPES_TO_PYARROW = {
        'float': pa.float32(),
        'int': pa.int32(),
        'string': pa.string(),
        'timestamp': pa.timestamp(unit='s')
    }

    def __init__(self, df, schema_resolver):
        self.df = df
        self._schema_resolver = schema_resolver
        self._validators = [NamesValidator, DtypesValidator]
        self._validate()

    def _validate(self):
        for validator in self._validators:
            validator(self.df, self._schema_resolver).validate()

    def _schema_to_pyarrow_schema(self):
        names_to_dtypes = self._schema_resolver.get_names_to_dtype()
        pyarrow_schema = {}
        for name, dtype in names_to_dtypes.items():
            pyarrow_schema[name] = self._DTYPES_TO_PYARROW[dtype]
        return pa.schema(pyarrow_schema)

    def to_pyarrow(self):
        pyarrow_schema = self._schema_to_pyarrow_schema()
        return pa.Table.from_pandas(df=self.df, schema=pyarrow_schema)
