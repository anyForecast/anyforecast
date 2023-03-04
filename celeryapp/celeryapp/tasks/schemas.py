import re
from typing import Dict, List


def upper_to_lower(string):
    return re.sub('(?<!^)(?=[A-Z])', '_', string).lower()


class FeaturesSchemaCreator:

    def __init__(self, schema_data: Dict):
        self._schema_data = schema_data

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

    def create_schema(self):
        names_map = self._create_names_map()
        return FeaturesSchema(**names_map)


class FeaturesSchema:
    def __init__(
            self, group_ids: List[str], timestamp: str, target: str,
            time_varying_known: List[str], time_varying_unknown: List[str],
            static_categoricals: List[str]
    ):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target
        self.time_varying_known = time_varying_known
        self.time_varying_unknown = time_varying_unknown
        self.static_categoricals = static_categoricals

    def update_schema(self, updates: Dict):
        for name, value in updates.items():
            setattr(self, name, value)
