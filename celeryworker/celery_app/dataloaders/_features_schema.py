import copy
import re


def upper_to_lower(string):
    return re.sub('(?<!^)(?=[A-Z])', '_', string).lower()


class FeaturesSchema:

    def __init__(self, schema_data):
        self._schema_data = schema_data
        self._names_map = self._create_names_map()

    def _create_names_map(self):
        singletons = ('Target', 'Timestamp')
        names_map = {}

        for k, features in self._schema_data.items():
            lower_k = upper_to_lower(k)
            if k in singletons:
                names_map[lower_k] = features[0]['FeatureName']
            else:
                names_map[lower_k] = [feat['FeatureName'] for feat in features]

        return names_map

    def get_names_for(self, labels):
        if labels == 'all':
            return copy.deepcopy(self._names_map)

        if isinstance(labels, str):
            return self._names_map[labels]
        return {l: self._names_map[l] for l in labels}
