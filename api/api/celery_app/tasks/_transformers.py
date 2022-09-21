import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


def make_transformer_from_run(name, run):
    transformers = {
        'datetime_locator': DatetimeLocator,
        'what_if': WhatIf
    }
    return transformers[name].from_run(run)


class DatetimeLocator(BaseEstimator, TransformerMixin):
    """Extends DataFrame rows until the given date range is achieved.

    If the passed ``date_range`` is already contained X,
    the rows inside it are returned. That is, in this case,
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

    def fit(self, X):
        return self

    def transform(self, X):
        Xt = X.set_index(self.timestamp_col).reindex(self.date_range)
        return Xt.fillna(method='ffill').fillna(method='bfill')


class GroupWiseDatetimeLocator(BaseEstimator, TransformerMixin):
    def __init__(
            self, group_ids, timestamp_col, date_range, reset_index=True
    ):
        self.group_ids = group_ids
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.reset_index = reset_index

    def fit(self, X):
        return self

    def transform(self, X):
        locator = DatetimeLocator(self.timestamp_col, self.date_range)

        X = X.groupby(self.group_ids, sort=False) \
            .apply(locator.fit_transform) \
            .drop(self.group_ids, axis='columns')

        if self.reset_index:
            return X.reset_index().rename(
                columns={'level_1': self.timestamp_col})
        return X

    @classmethod
    def from_run(cls, run):
        encoder_date_range, forecast_date_range = run.split_date_range()
        full_date_range = encoder_date_range.union(forecast_date_range)
        group_ids = run.get_names_for('group_ids')
        timestamp = run.get_names_for('timestamp')[0]
        return GroupWiseDatetimeLocator(group_ids, timestamp, full_date_range)


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

    @classmethod
    def from_run(cls, run):
        what_if_data = run.group_params_resolver.get_what_if_data()
        group_ids = run.get_names_for('group_ids')
        timestamp = run.get_names_for('timestamp')[0]
        _, forecast_date_range = run.split_date_range()
        return cls(group_ids, forecast_date_range, timestamp, what_if_data)
