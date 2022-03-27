import pyarrow as pa

from ...exceptions import (
    MissingGroupIds,
    TypeNotSupported,
    MissingFeatureKey,
    TypesMismatch,
    DatasetTypeNotKnown,
    DistinctNumberOfFeatures,
    MissingTimestamp,
    MissingTarget,
    ExtraFeatures
)


def make_dataset(df, schema, dataset_type):
    dataset_map = {
        'target': TargetDataset,
    }

    try:
        return dataset_map[dataset_type](df, schema)
    except KeyError:
        raise DatasetTypeNotKnown(type=dataset_type,
                                  available=list(dataset_map))


class Dataset:
    """Base class for datasets. Collects validations common for all derived
    Datasets.

    A dataset consists on a dataframe and its schema.

    Parameters
    ----------
    df : pd.DataFrame
    schema : dict

    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self, df, schema):
        self.df = df
        self.schema = schema
        self._validate()

    def _validate(self):
        """Validates dataframe matches the given schema
        """
        self._validate_schema_features_keys()
        self._validate_schema_group_ids()
        self._validate_schema_types()
        self._validate_schema_timestamp()
        self._validate_df_features()
        self._validate_df_types()

    def _validate_schema_features_keys(self):
        """Validate schema contains the needed keys and saves feature names.
        """
        if 'Features' not in self.schema:
            raise ValueError('"Features" missing in schema.')

        feature_names = []
        needed_keys = ['FeatureName', 'FeatureType']
        for i, feature_data in enumerate(self.schema['Features']):
            for key in needed_keys:
                if key not in feature_data:
                    raise MissingFeatureKey(key=key, position=i)

            feature_name = feature_data['FeatureName']
            feature_names.append(feature_name)

        self._feature_names = feature_names

    def _validate_schema_group_ids(self):
        """Validates schema contains group ids and saves group_ids.
        """
        group_ids = []
        for feature_data in self.schema['Features']:
            feature_name = feature_data['FeatureName']
            if feature_name.startswith('group_id'):
                group_ids.append(feature_name)

        if not group_ids:
            raise MissingGroupIds()

        self._group_ids = group_ids

    def _validate_schema_types(self):
        """Validates schema contains supported types and saves them.
        """
        supported_types = [
            'float',
            'int',
            'string',
            'timestamp'
        ]
        schema_types = {}
        for feature_data in self.schema['Features']:
            feature_name = feature_data['FeatureName']
            feature_type = feature_data['FeatureType']
            schema_types[feature_name] = feature_type
            if feature_type not in supported_types:
                raise TypeNotSupported(name=feature_name, type=feature_type,
                                       supported_types=supported_types)

        self._schema_types = schema_types

    def _validate_schema_timestamp(self):
        """Validates schema contains the timestamp feature.
        """
        if 'timestamp' not in self._schema_types:
            raise MissingTimestamp()

    def _validate_df_types(self):
        """Validates df dtypes coincide schema types.
        """
        # Mapping from supported schema types to pandas dtypes.
        types_map = {
            'float': ['float64', 'float32'],
            'int': ['int64', 'int32'],
            'string': ['object'],
            'timestamp': ['datetime64[ns]']
        }

        dtypes_dict = self.df.dtypes.to_dict()
        for feature_data in self.schema['Features']:
            feature_name = feature_data['FeatureName']
            feature_type = feature_data['FeatureType']
            dtype = dtypes_dict[feature_name]
            if dtype not in types_map[feature_type]:
                raise TypesMismatch(type=feature_type, dtype=dtype)

    def _validate_df_features(self):
        """Validates df contains the same schema features.
        """
        n_schema = len(self._feature_names)
        n_df = len(self.df.columns)
        if n_schema != n_df:
            raise DistinctNumberOfFeatures(n_schema=n_schema, n_df=n_df)

        for name in self._feature_names:
            if name not in self.df:
                raise

    def _schema_to_pyarrow_schema(self):
        # Mapping from supported schema types to pyarrow DataTypes.
        types_map = {
            'float': pa.float32(),
            'int': pa.int32(),
            'string': pa.string(),
            'timestamp': pa.timestamp(unit='s')
        }
        pyarrow_schema = {}
        for feature_data in self.schema['Features']:
            feature_name = feature_data['FeatureName']
            feature_type = feature_data['FeatureType']
            pyarrow_schema[feature_name] = types_map[feature_type]
        return pa.schema(pyarrow_schema)

    def to_pyarrow(self):
        pyarrow_schema = self._schema_to_pyarrow_schema()
        return pa.Table.from_pandas(df=self.df, schema=pyarrow_schema)


class TargetDataset(Dataset):
    def __init__(self, df, schema):
        super().__init__(df, schema)
        self._validate()

    @property
    def type(self):
        return 'target'

    def _validate(self):
        super()._validate()
        self._validate_target_feature()
        self._validate_no_extra_features()

    def _validate_target_feature(self):
        if 'target' not in self._feature_names:
            raise MissingTarget()

    def _validate_no_extra_features(self):
        supported_features = self._group_ids + ['timestamp', 'target']
        extra_features = set(self._feature_names) - set(supported_features)
        if extra_features:
            raise ExtraFeatures(extras=extra_features)


class RelatedDataset(Dataset):
    def __init__(self, df, schema):
        super().__init__(df, schema)

    @property
    def type(self):
        return 'related'
