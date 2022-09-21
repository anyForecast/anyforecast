import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class WhatIf(BaseEstimator, TransformerMixin):

    def __init__(self, date_range, timestamp_col, column, value=None,
                 method=None, percentage=None):
        self.date_range = date_range
        self.timestamp_col = timestamp_col
        self.column = column
        self.value = value
        self.method = method
        self.percentage = percentage

    def fit(self, X):
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
        X.loc[mask, self.column] = np.nan
        return X

    def _percentage_change(self, X, mask):
        factor = 1 + (self.percentage / 100)

        X.loc[mask, self.column] *= factor
        return X


class GroupWiseWhatIf(BaseEstimator, TransformerMixin):
    def __init__(self, group_ids, date_range, timestamp_col, what_if_data):
        self.group_ids = group_ids
        self.date_range = date_range
        self.timestamp_col = timestamp_col
        self.what_if_data = what_if_data

    def fit(self, X):
        return self

    def transform(self, X):
        groupby = X.groupby(self.group_ids, sort=False)
        groups_yielder = self._groups_yielder(groupby.groups)
        return groupby.apply(self._transform, groups_yielder=groups_yielder)

    def _groups_yielder(self, groups):
        for g in groups:
            yield g

    def _transform(self, X, groups_yielder):
        group_id = next(iter(groups_yielder))

        try:
            data = self.what_if_data[group_id]
        except KeyError:
            return X

        what_if = WhatIf(self.date_range, self.timestamp_col, **data)
        return what_if.fit_transform(X)
