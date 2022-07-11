from .exceptions import UnknownFeatureTypeError


class Feature:
    def __init__(self, dtype):
        self.dtype = dtype


class MetaFeature(Feature):
    def __init__(self, dtype):
        super().__init__(dtype)


class RelatedFeature(Feature):
    def __init__(self, dtype):
        super().__init__(dtype)


class GroupId(MetaFeature):
    def __init__(self):
        super().__init__(dtype='string')


class Timestamp(MetaFeature):
    def __init__(self):
        super().__init__(dtype='timestamp')


class Target(MetaFeature):
    def __init__(self):
        super().__init__(dtype='float')


class TimeVaryingKnown(RelatedFeature):
    def __init__(self):
        super().__init__(dtype='float')


class TimeVaryingUnknown(RelatedFeature):
    def __init__(self):
        super().__init__(dtype='float')


class StaticCategorical(RelatedFeature):
    def __init__(self):
        super().__init__(dtype='string')


_FEATURES_CATALOG = {
    'time_varying_known': TimeVaryingKnown,
    'time_varying_unknown': TimeVaryingUnknown,
    'static_categorical': StaticCategorical,
    'group_id': GroupId,
    'timestamp': Timestamp,
    'target': Target
}


def _get_feature_class(key):
    try:
        return _FEATURES_CATALOG[key]
    except KeyError:
        raise UnknownFeatureTypeError(feature_type=key)


def make_feature(feature_type):
    """Factory function for :class:`Feature` objects.

    Parameters
    ----------
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

    Returns
    -------
    feature: Feature
    """
    return _get_feature_class(key=feature_type)()


class FeatureLocator:
    def __init__(self):
        self._features = {}

    def register_feature(self, name, feature_type):
        feature = make_feature(feature_type)
        self._features[name] = feature

    def get_by_type(self):
        by_type = {}
        for name, feature in self._features.items():
            # The feature type is given by the class name.
            feature_type = feature.__class__.__name__
            feature_dict = self._create_feature_dict(name, feature.dtype)
            if feature_type not in by_type:
                by_type[feature_type] = [feature_dict]
            else:
                by_type[feature_type].append(feature_dict)
        return by_type

    def get_all_features(self):
        return self._features

    def get_feature(self, name):
        try:
            return self._features[name]
        except KeyError:
            raise ValueError("Unknown feature: %s" % name)

    def _create_feature_dict(self, name, dtype):
        return {'FeatureName': name, 'FeatureDtype': dtype}
