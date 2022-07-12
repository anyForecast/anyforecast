from ._features import FeatureLocator
from .writers import JsonWriter


class SchemaCreator:
    """Creator of :class:`Schema` objects.

    Parameters
    ----------
    group_ids : list of str
        List of column names identifying a time series. This means that the
        ``group_ids`` identify a sample together with the ``timestamp``.
        If you have only one time series, set this to the name of column that
        is constant.

    timestamp : str
        Timestamp feature.

    target : str
        Target feature. Feature containing the values to be predicted.
    """

    def __init__(self, group_ids, timestamp, target):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target

        self._features_locator = FeatureLocator()
        self._register_pk_features()
        self._register_target()

    def _register_pk_features(self):
        self._register_group_ids()
        self._register_timestamp()

    def _register_timestamp(self):
        self._features_locator.register_feature(self.timestamp, 'timestamp')

    def _register_group_ids(self):
        for group_id in self.group_ids:
            self._features_locator.register_feature(group_id, 'group_id')

    def _register_target(self):
        self._features_locator.register_feature(self.target, 'target')

    def add_feature(self, name, feature_type):
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
        self._features_locator.register_feature(name, feature_type)

    def create_schema(self):
        """Creates :class:`Schema` object.

        Returns
        -------
        schema : client.schema.Schema
            :class:`Schema` object.
        """
        features_data = self._features_locator.get_by_type()
        return Schema(features_data)


class Schema:
    def __init__(self, features_data):
        self._features_data = features_data
        self.write = JsonWriter(self.to_dict())

    def to_dict(self):
        return self._features_data

    def get_names_to_dtype(self):
        names_to_dtype = {}
        for _, features in self._features_data.items():
            for feat in features:
                name = feat['FeatureName']
                dtype = feat['FeatureDtype']
                names_to_dtype[name] = dtype
        return names_to_dtype

    def get_names(self):
        return list(self.get_names_to_dtype())
