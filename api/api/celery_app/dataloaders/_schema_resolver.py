import re
from itertools import chain


def upper_to_lower(string):
    return re.sub('(?<!^)(?=[A-Z])', '_', string).lower()


class FeaturesData:
    def __init__(self, features_data):
        self.features_data = features_data

    @property
    def names(self):
        return [feature['FeatureName'] for feature in self.features_data]

    @property
    def dtypes(self):
        return {
            feature['FeatureName']: feature['FeatureDtype']
            for feature in self.features_data
        }


class SchemaResolver:

    def __init__(self, schema_data):
        self.schema_data = schema_data
        self._set_attributes()

    def _set_attributes(self):
        for k, features_data in self.schema_data.items():
            features_data_obj = FeaturesData(features_data)
            lower_k = upper_to_lower(k)
            setattr(self, lower_k, features_data_obj)

    def get_feature_names(self):
        return list(chain.from_iterable(self.get_names_for('all').values()))

    def get_names_for(self, keys, exclude=None):
        if keys == 'all':
            keys = self._get_all_keys()

        if exclude is not None:
            self._exclude(keys, exclude)

        names = {}
        for k in keys:
            feature_data_obj = getattr(self, k)
            names[k] = feature_data_obj.names
        return names

    def get_dtypes_for(self, keys, exclude=None):
        if keys == 'all':
            keys = self._get_all_keys()

        if exclude is not None:
            self._exclude(keys, exclude)

        dtypes = {}
        for k in keys:
            feature_data_obj = getattr(self, k)
            dtypes.update(feature_data_obj.dtypes)
        return dtypes

    def _get_all_keys(self):
        return [upper_to_lower(k) for k in self.schema_data]

    def _exclude(self, ls, exclude):
        if not isinstance(exclude, list):
            exclude = [exclude]

        for exc in exclude:
            if exc in ls:
                ls.remove(exc)
