from .exceptions import InvalidFeatureType
from .features import FeatureLocator
from .services.write import JsonWriter


class SchemaCreator:
    """Creator of :class:`Schema` objects.

    Parameters
    ----------
    group_ids : list of str
        List of column names identifying a time series. This means that the
        ``group_ids`` identify a sample together with the ``timestamp``.
        If you have only one time series, set this to the name of column that
        is constant.

    timestamp_col : str
        Timestamp column/feature.

    target_col : str
        Target column/feature. Feature containing the values to be predicted.
    """

    def __init__(self, group_ids, target_col, timestamp_col):
        self.group_ids = group_ids
        self.target_col = target_col
        self.timestamp_col = timestamp_col
        self._features_locator = FeatureLocator()
        self._register_init_features()

    def _register_init_features(self):
        self._register_group_ids()
        self._register_timestamp()
        self._register_target()

    def _register_group_ids(self):
        for group_id in self.group_ids:
            self._features_locator.register_feature(group_id, 'group_id')

    def _register_timestamp(self):
        if self.timestamp_col is not None:
            self._features_locator.register_feature(
                self.timestamp_col, 'timestamp')

    def _register_target(self):
        self._features_locator.register_feature(self.target_col, 'target')

    def register_feature(self, name, feature_type, dtype=None, validate=True):
        """Adds feature to schema.

        Parameters
        ----------
        name : str
            Feature name.

        feature_type : str, {'time_varying_known', 'time_varying_unknown',
        'static_categorical'}
            Feature type. Description for each:

            - time_varying_known: List of continuous variables that change over
            time and are known in the future (e.g. price of a product, but
            not demand of a product).

            - time_varying_unknown: List of continuous variables that change
            over time and are not known in the future.

            - static_categoricals: List of categorical variables that do not
            change over time (also known as `time independent variables`).
        """
        self._features_locator.register_feature(name, feature_type, dtype,
                                                validate)

    def create_schema(self):
        """Creates :class:`Schema` object.

        Returns
        -------
        schema : client.schema.Schema
            :class:`Schema` object.
        """
        features_data = self._features_locator.get_features_data()
        return Schema(features_data)


class Schema:
    """Schema.

    Holds information for all feature types and dtypes.

    Parameters
    ----------
    features_data : dict
        mapping from feature type to a list of features
        having such type. Currently, the following feature types exist and all
        must be included: TimeVaryingKnown,TimeVaryingUnknown,
        StaticCategoricals, GroupIds, Timestamp, Target.
    """

    def __init__(self, features_data):
        self.features_data = features_data

    def dict(self):
        """Converts features data, i.e., ``self.features_data`` into a
        serializable format.

        Returns
        -------
        features_data : dict, str -> list of dict
        """
        return {
            k: [dict(f) for f in features_list]
            for k, features_list in self.features_data.items()
        }

    @property
    def write(self):
        return JsonWriter(self.dict())

    def get_dtypes(self):
        names_to_dtype = {}
        for _, features in self.features_data.items():
            for feat in features:
                name = feat.FeatureName
                dtype = feat.FeatureDtype
                names_to_dtype[name] = dtype
        return names_to_dtype

    def get_names_by_feature_type(self, feature_type):
        try:
            features = self.features_data[feature_type]
        except KeyError:
            raise InvalidFeatureType(feature_type=feature_type)

        names = []
        for feat in features:
            name = feat.FeatureName
            names.append(name)
        return names

    def get_names(self):
        return list(self.get_dtypes())
