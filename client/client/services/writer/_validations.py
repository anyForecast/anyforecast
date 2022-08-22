from abc import abstractmethod, ABCMeta

from ...exceptions import (
    TypesMismatch,
    ExtraFeaturesInPandasError,
    ExtraFeaturesInSchemaError
)


def validate_dataframe_and_schema(dataframe, schema):
    """Validates data and schema match in names and dtypes.

    Resume of validations:
     - A = B where A and B are the set of features name define in the
        schema and the names of the pandas dataframe columns, respectively.

    - Dtypes from pandas dataframe match the ones defined in the schema.

    Parameters
    ----------
    dataframe : pd.DataFrame

    schema : client.schema.Schema
        :class:`Schema` object.
    """

    validators = [NamesValidator, DtypesValidator]
    for validator in validators:
        validator(dataframe, schema).validate()


class DatasetValidator(metaclass=ABCMeta):
    def __init__(self, df, schema):
        self.df = df
        self.schema = schema

    @abstractmethod
    def validate(self):
        pass


class DtypesValidator(DatasetValidator):

    def __init__(self, df, schema):
        super().__init__(df, schema)

    def validate(self):
        schema_dtypes = self.schema.get_dtypes()
        df_dtypes = self.df.get_dtypes()
        for name, schema_dtype in schema_dtypes.items():
            df_dtype = df_dtypes[name]
            self._validate_dtype(name, schema_dtype, df_dtype)

    def _validate_dtype(self, name, schema_dtype, pandas_dtype):
        schema_dtype_to_pandas_dtype = {
            'float': ['float64', 'float32'],
            'int': ['int64', 'int32'],
            'object': ['object'],
            'timestamp': ['datetime64[ns]']
        }
        if pandas_dtype not in schema_dtype_to_pandas_dtype[schema_dtype]:
            raise TypesMismatch(name=name, schema_dtype=schema_dtype,
                                pandas_dtype=pandas_dtype)


class NamesValidator(DatasetValidator):
    def __init__(self, df, schema_resolver):
        super().__init__(df, schema_resolver)

    def validate(self):
        names_set = set(self.schema.get_names())
        pandas_set = set(self.df.get_names())

        if not pandas_set.issubset(names_set):
            extras = pandas_set.difference(names_set)
            raise ExtraFeaturesInPandasError(extras=extras)

        if not names_set.issubset(pandas_set):
            extras = names_set.difference(pandas_set)
            raise ExtraFeaturesInSchemaError(extras=extras)
