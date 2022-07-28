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
        names_to_dtype = self.schema.get_names_to_dtype()
        for name, dtype in names_to_dtype.items():
            self._validate_dtype(name, dtype)

    def _validate_dtype(self, name, dtype):
        """Validates df dtypes coincide schema types.
        """
        dtype_to_pandas = {
            'float': ['float64', 'float32'],
            'int': ['int64', 'int32'],
            'string': ['object'],
            'timestamp': ['datetime64[ns]']
        }
        pandas_dtype = self._get_pandas_dtype(name)
        if pandas_dtype not in dtype_to_pandas[dtype]:
            raise TypesMismatch(name=name,
                                schema_dtype=dtype,
                                pandas_dtype=pandas_dtype)

    def _get_pandas_dtype(self, col_name):
        return self.df.get_dtypes()[col_name]


class NamesValidator(DatasetValidator):
    def __init__(self, df, schema_resolver):
        super().__init__(df, schema_resolver)

    def validate(self):
        names_set = set(self.schema.get_names())
        pandas_set = set(self.df.get_names())

        if not names_set.issubset(pandas_set):
            extras = names_set.difference(pandas_set)
            raise ExtraFeaturesInSchemaError(extras=extras)

        if not pandas_set.issubset(names_set):
            extras = pandas_set.difference(names_set)
            raise ExtraFeaturesInPandasError(extras=extras)
