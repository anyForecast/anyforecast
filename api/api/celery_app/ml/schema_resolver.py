import re

import pyarrow as pa


def upper_to_lower(string):
    return re.sub('(?<!^)(?=[A-Z])', '_', string).lower()


class MultiFeature:
    def __init__(self, *args):
        self._args = args

    @property
    def names_to_dtype(self):
        if self._args[0].name is None:
            return {}

        n_to_d = {}
        for feature in self._args:
            n_to_d[feature.name] = feature.dtype
        return n_to_d

    @property
    def names(self):
        if self._args[0].name is None:
            return []
        return [feature.name for feature in self._args]


class Feature:
    def __init__(self, name, dtype):
        self.name = name
        self.dtype = dtype


class SchemaResolver:
    KEYS = [
        "GroupIds",
        "Timestamp",
        "Target",
        "TimeVaryingKnown",
        "TimeVaryingUnknown",
        "StaticCategoricals"
    ]

    def __init__(self, schema_data):
        self.schema_data = schema_data
        for key in self.KEYS:
            self._set_feature(key)

    def get_names_for(self, keys):
        return {k: getattr(self, k).names for k in keys}

    def to_arrow(self):
        # Mapping from supported schema types to pyarrow DataTypes.
        types_map = {
            'float': pa.float32(),
            'int': pa.int32(),
            'string': pa.string(),
            'timestamp': pa.timestamp(unit='s')
        }
        names_to_dtypes = self._get_names_to_dtypes()
        names_to_arrow_dtypes = {
            name: types_map[dtype] for name, dtype in names_to_dtypes.items()
        }
        return pa.schema(names_to_arrow_dtypes)

    def _get_names_to_dtypes(self):
        dtypes = {}
        keys = [upper_to_lower(k) for k in self.KEYS]
        for k in keys:
            n_to_d = getattr(self, k).names_to_dtype
            if n_to_d:
                dtypes.update(n_to_d)
        return dtypes

    def _set_feature(self, key):
        if key not in self.schema_data:
            self._set_empty_key(key)
        else:
            self._set_non_empty_key(key)

    def _set_non_empty_key(self, key):
        features_data = self.schema_data[key]
        feat_objects = []
        for feature in features_data:
            name = feature['FeatureName']
            dtype = feature['FeatureDtype']
            feat_obj = Feature(name, dtype)
            feat_objects.append(feat_obj)

        self._set_feature_objs(key, feat_objects)

    def _set_empty_key(self, key):
        empty_feat = Feature(name=None, dtype=None)
        self._set_feature_objs(key, [empty_feat])

    def _set_feature_objs(self, key, objs):
        attr_name = upper_to_lower(key)
        multi_feature = MultiFeature(*objs)
        setattr(self, attr_name, multi_feature)
