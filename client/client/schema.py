from ._features import FeatureLocator


class SchemaCreator:

    def __init__(self, group_ids, timestamp, target):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target

        self._meta_locator = FeatureLocator()
        self._features_locator = FeatureLocator()
        self._register_meta_features()
        self._register_target()

    def _register_meta_features(self):
        self._register_group_ids()
        self._register_timestamp()

    def _register_timestamp(self):
        self._meta_locator.register_feature(self.timestamp, 'timestamp')

    def _register_group_ids(self):
        for group_id in self.group_ids:
            self._meta_locator.register_feature(group_id, 'group_id')

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
        meta_schema = self._meta_locator.get_by_type()
        features_schema = self._features_locator.get_by_type()
        return {'Meta': meta_schema, 'Features': features_schema}


class SchemaResolver:
    def __init__(self, schema):
        self.schema = schema

    def get_names_to_dtype(self, include_meta=True):
        features = self._get_features_for('Features')
        if include_meta:
            meta_features = self._get_features_for('Meta')
            features = meta_features + features

        names_to_dtype = {}
        for feat in features:
            name = feat['FeatureName']
            dtype = feat['FeatureDtype']
            names_to_dtype[name] = dtype
        return names_to_dtype

    def get_names(self, include_meta=True):
        return list(self.get_names_to_dtype(include_meta))

    def _get_features_for(self, key):
        features = []
        key_data = self.get(key)
        for _, feats in key_data.items():
            features.extend(feats)
        return features

    def get(self, key):
        return self.schema[key]






