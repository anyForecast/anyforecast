import copy

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.exceptions import NotFittedError
from sklearn.pipeline import make_pipeline
from sklearn.utils.validation import check_is_fitted


def _is_fitted(estimator):
    try:
        check_is_fitted(estimator)
    except NotFittedError:
        return False
    return True


class FeatureNamesTransformer(BaseEstimator, TransformerMixin):
    """

    Parameters
    ----------
    preprocessor : sklearn Pipeline
        Fitted preprocessor.

    steps_to_ignore : list of str, default=None
    """

    def __init__(
            self, preprocessor, steps_to_ignore=None, ignore_remainder=True
    ):
        check_is_fitted(preprocessor)
        self.preprocessor = preprocessor
        self.steps_to_ignore = steps_to_ignore
        self.ignore_remainder = ignore_remainder

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Do not alter original dict.
        X = copy.deepcopy(X)

        steps_to_ignore = (
            self.steps_to_ignore if self.steps_to_ignore is not None
            else []
        )

        for step_name, step in self.preprocessor.named_steps.items():
            if step_name not in steps_to_ignore:
                for name, trans, features_in, _ in step.iter():
                    if (
                            (name == 'remainder' and self.ignore_remainder) or
                            not _is_fitted(trans)
                    ):
                        continue

                    features_in = self._get_features_in(features_in, trans)
                    features_out = self._get_features_out(trans)
                    if features_in.tolist() != features_out.tolist():
                        X = self._transform_names(X, features_in, features_out)

        return X

    def _transform_names(self, X, features_in, features_out):
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

    def _is_name_to_ignore(self, name):
        return (name == 'remainder' and self.ignore_remainder) or (
                name in self.steps_to_ignore)


class DatetimeLocator(BaseEstimator, TransformerMixin):
    """Locates DataFrame rows by date range.

    If the passed ``date_range`` is already contained X,
    the rows inside it are returned. That is, in such case,
    this transformation is equivalent to a simple datetime filter.

    Parameters
    ----------
    timestamp_col : str
        Datetime column

    date_range : pd.DatetimeIndex
        Instance of :class:`pd.DatetimeIndex` obtained from
        :meth:`pd.date_range()`.
    """

    def __init__(self, timestamp_col, date_range):
        self.timestamp_col = timestamp_col
        self.date_range = date_range

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return self._transform(X)

    def _transform(self, X):
        Xt = X.set_index(self.timestamp_col).reindex(self.date_range)
        return Xt.fillna(method='ffill').fillna(method='bfill')


class GroupWiseDatetimeLocator(BaseEstimator, TransformerMixin):
    def __init__(self, timestamp_col, date_range, group_ids=None,
                 reset_index=True):
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.group_ids = group_ids
        self.reset_index = reset_index

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        dt_locator = DatetimeLocator(self.timestamp_col, self.date_range)

        X = X.groupby(self.group_ids, sort=False) \
            .apply(dt_locator.transform) \
            .drop(self.group_ids, axis='columns')

        if self.reset_index:
            return X.reset_index().rename(
                columns={'level_1': self.timestamp_col})
        return X


class WhatIfTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, timestamp_col, date_range, input_cols, value=None,
                 method=None, percentage=None):
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.input_cols = input_cols
        self.value = value
        self.method = method
        self.percentage = percentage

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        mask = self._make_bool_mask(X)

        if self.percentage is not None:
            return self._percentage_change(X, mask)

        X = self._place_nans(X, mask)
        return X.fillna(value=self.value, method=self.method)

    def _make_bool_mask(self, X):
        return X[self.timestamp_col].isin(self.date_range)

    def _place_nans(self, X, mask):
        X.loc[mask, self.input_cols] = np.nan
        return X

    def _percentage_change(self, X, mask):
        factor = 1 + (self.percentage / 100)

        X.loc[mask, self.input_cols] *= factor
        return X


class BulkWhatIfTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, group_ids, timestamp_col, date_range, what_if_data):
        self.group_ids = group_ids
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.what_if_data = what_if_data

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        groupby = X.groupby(self.group_ids)
        for what_if in self.what_if_data:
            group = self._loc_group(groupby, what_if)
            what_if_transformer = self._make_what_if_transformer(what_if)
            transformed_group = what_if_transformer.fit_transform(group)

            # Overwrite transformed values.
            X.loc[group.index] = transformed_group.values

        return X

    def _loc_group(self, groupby, what_if):
        group_id = what_if.pop('group_id')
        group_id = [group_id[g] for g in self.group_ids]
        group_id = group_id[0] if len(group_id) == 1 else tuple(group_id)
        return groupby.get_group(group_id)

    def _make_what_if_transformer(self, what_if):
        return WhatIfTransformer(self.date_range, self.timestamp_col, **what_if)


class InferenceTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, group_ids, timestamp_col, date_range, what_if_data):
        self.group_ids = group_ids
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.what_if_data = what_if_data

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        pipeline = self._make_pipeline()
        return pipeline.fit_transform(X)

    def _make_pipeline(self):
        steps = [
            self._make_datetime_locator(),
            self._make_what_if_transformer()
        ]
        return make_pipeline(*steps)

    def _make_what_if_transformer(self):
        return BulkWhatIfTransformer(
            self.group_ids, self.timestamp_col, self.date_range,
            self.what_if_data)

    def _make_datetime_locator(self):
        return GroupWiseDatetimeLocator(
            self.timestamp_col, self.date_range, group_ids=self.group_ids)
