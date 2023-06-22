import copy
from dataclasses import dataclass

import numpy as np
from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline





class TimeseriesPreprocessor:
    """Timeseries preprocessor creators.

    Aims to facilitate the construction of a timeseries preprocessor by
    combining group-wise and non group-wise transformations in a sklearn
    :class:`Pipeline`.

    Parameters
    ----------
    """

    def __init__(self, ts_schema: TimeseriesSchema, freq):
        self.ts_schema = ts_schema
        self.freq = freq

    def create(self):
        pass

    def update_schema(self):
        ts_schema = copy.deepcopy(self.ts_schema)

        for key, values in vars(ts_schema).items():
            values = self.transform(values)
            setattr(ts_schema, key, values)

        return ts_schema


class NamesTransformer(BaseEstimator):
    """

    Parameters
    ----------
    preprocessor : sklearn Pipeline
        Fitted preprocessor.

    steps_to_ignore : list of str, default=None
    """

    def __init__(
            self,
            pipeline: Pipeline,
            steps_to_ignore=None,
            ignore_remainder=True
    ):

        self.pipeline = pipeline
        self.steps_to_ignore = steps_to_ignore
        self.ignore_remainder = ignore_remainder

    def transform(self, names):
        steps_to_ignore = self.steps_to_ignore or []

        for step_name, step in self.preprocessor.named_steps.items():
            if step_name not in steps_to_ignore:
                for name, trans, features_in, _ in step.iter():

                    if self._continue(name, trans):
                        continue

                    features_in = self._get_features_in(features_in, trans)
                    features_out = self._get_features_out(trans)
                    if features_in.tolist() != features_out.tolist():
                        X = self.transform_names(X, features_in, features_out)

        return X

    def _continue(self, name, trans) -> bool:
        """Continuation policy.

        Continue/skip iteration if either
        1. name=="remainder" AND ignore_remainder is True
        2. trans if not fitted
        """
        return (
                (name == 'remainder' and self.ignore_remainder)
                or not _is_fitted(trans)
        )

    def transform_names(self, X, features_in, features_out):
        for feature_type, names in X.items():
            if self._check_multi_membership(names, features_in):
                names = self._bulk_remove(names, features_in)
                names = self._bulk_insert(names, features_out)
                X[feature_type] = names
                break
        return X

    def _get_features_out(self, trans):
        """Obtains features out from transformer.
        """
        return trans.get_feature_names_out()

    def _get_features_in(self, features_in, trans):
        """Tries to obtain ``features_in`` directly from transformer.
        If not possible returns the one from meth:`step.iter()`.
        """
        try:
            features_in = trans.feature_names_in_
        except AttributeError:
            if not isinstance(features_in, list):
                features_in = np.array([features_in])
        return features_in

    def _check_multi_membership(self, ls, items):
        """Checks the membership of multiple values at once in a list
        """
        return all(x in ls for x in items)

    def _bulk_remove(self, ls, to_remove):
        """Removes multiple items from a list.
        """
        return [x for x in ls if x not in to_remove]

    def _bulk_insert(self, ls, to_insert):
        """Inserts multiple items to a list.
        """
        return ls + list(to_insert)
