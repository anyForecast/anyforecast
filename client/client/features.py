from .exceptions import InvalidFeatureType


class Feature:
    """Base class for Features.
    """
    def __init__(self, FeatureName, FeatureDtype):
        self.FeatureName = FeatureName
        self.FeatureDtype = FeatureDtype
        self._checkFeatureDtype(FeatureDtype)

    def keys(self):
        return ["FeatureName", "FeatureDtype"]

    def __getitem__(self, key):
        return getattr(self, key)

    def _checkFeatureDtype(self, FeatureDtype):
        allowed_dtypes = ['object', 'int', 'float', 'timestamp', 'string']
        if FeatureDtype not in allowed_dtypes:
            raise


class GroupIds(Feature):
    """Group ids feature.

    Parameters
    ----------
    name : str
        Feature name.
    """
    def __init__(self, name):
        super().__init__(FeatureName=name, FeatureDtype='object')


class Timestamp(Feature):
    """Timestamp feature.

    Parameters
    ----------
    name : str
        Feature name.
    """
    def __init__(self, name):
        super().__init__(FeatureName=name, FeatureDtype='timestamp')


class Target(Feature):
    """Target feature.

    Parameters
    ----------
    name : str
        Feature name.
    """
    def __init__(self, name):
        super().__init__(FeatureName=name, FeatureDtype='float')


class TimeVaryingKnown(Feature):
    """Time varying known feature.

    Continuous variables that change over
    time and are known in the future (e.g. price of a product, but
    not demand of a product).

    Parameters
    ----------
    name : str
        Feature name.

    dtype : str, default='float'
        Feature dtype.
    """
    def __init__(self, name, dtype='float'):
        super().__init__(FeatureName=name, FeatureDtype=dtype)


class TimeVaryingUnknown(Feature):
    """Time varying unknown feature.

    Continuous variables that change over time and are not known in the future.
    For example, the Target variable.

    Parameters
    ----------
    name : str
        Feature name.

    dtype : str, default='float'
        Feature dtype.
    """
    def __init__(self, name, dtype='float'):
        super().__init__(FeatureName=name, FeatureDtype=dtype)


class StaticCategoricals(Feature):
    """Static categorical variables.

    Variables that do not change over time (also known as
    `time independent variables`).

    Parameters
    ----------
    name : str
        Feature name.

    dtype : str, default='object'
        Feature dtype.
    """
    def __init__(self, name, dtype='object'):
        super().__init__(FeatureName=name, FeatureDtype=dtype)


_FEATURES_MAP = {
    'time_varying_known': TimeVaryingKnown,
    'time_varying_unknown': TimeVaryingUnknown,
    'static_categorical': StaticCategoricals,
    'group_id': GroupIds,
    'timestamp': Timestamp,
    'target': Target
}

FEATURE_TYPES = [x.__name__ for x in list(_FEATURES_MAP.values())]


def make_feature(name, feature_type, dtype=None):
    """Factory function for :class:`Feature` objects.

    Parameters
    ----------
    name : str,
        Feature name

    feature_type : str, {'time_varying_known', 'time_varying_unknown',
        'static_categorical'}
        Feature type. Description for each:

        - time_varying_known: Continuous variables that change over
        time and are known in the future (e.g. price of a product, but
        not demand of a product).
        Default dtype: 'float'.

        - time_varying_unknown: Continuous variables that change
        over time and are not known in the future.
        Default dtype: 'float'.

        - static_categoricals: Categorical variables that do not
        change over time (also known as `time independent variables`).
        Default dtype: 'object'.

    dtype : str, {'string', 'int', 'float', 'object', 'timestamp'} default=None
        Feature dtype. If None, the default dtype of each feature type is used.


    Returns
    -------
    feature: Feature
    """
    try:
        return _FEATURES_MAP[feature_type](name, dtype)
    except KeyError:
        raise InvalidFeatureType(feature_type=feature_type)


class FeatureLocator:
    def __init__(self):
        self._features = []

    def register_feature(self, name, feature_type, dtype=None):
        """Registers feature.

        Parameters
        ----------
        name : str
            Feature name

        feature_type : str, {'time_varying_known', 'time_varying_unknown',
        'static_categorical'}
            Feature type. Description for each:

            - time_varying_known: Continuous variables that change over
            time and are known in the future (e.g. price of a product, but
            not demand of a product).
            Default dtype: 'float'.

            - time_varying_unknown: Continuous variables that change
            over time and are not known in the future.
            Default dtype: 'float'.

            - static_categoricals: Categorical variables that do not
            change over time (also known as `time independent variables`).
            Default dtype: 'object'.

        dtype : str, {'string', 'int', 'float', 'object', 'timestamp'},
         default=None
            Feature dtype. If None, the default dtype of each feature type is
            used.
        """
        feature = make_feature(name, feature_type, dtype)
        self._features.append(feature)

    def get_features_data(self):
        """Returns features data.

        ``features_data`` is a mapping from feature type to a list of features
        having such type. Currently, the following feature types exist:
        TimeVaryingKnown,TimeVaryingUnknown, StaticCategoricals,
        GroupIds, Timestamp, Target.

        Returns
        -------
        map : dict, str -> list of str
        """
        feature_types = self._get_feature_types()
        features_by_type = {k: [] for k in feature_types}

        for feature in self._features:
            # The ``feature_type`` is given by the class name.
            cls_name = feature.__class__.__name__
            features_by_type[cls_name].append(dict(feature))
        return features_by_type

    def get_features(self):
        return self._features

    def get_feature_by_name(self, name):
        for feature in self._features:
            if feature.FeatureName == name:
                return feature
        else:
            raise ValueError("Unknown feature: %s" % name)

    def _get_feature_types(self):
        return FEATURE_TYPES
