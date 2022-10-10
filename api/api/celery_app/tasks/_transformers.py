from abc import abstractmethod

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


class PredictionTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, timestamp_col, date_range, group_ids=None):
        self.timestamp_col = timestamp_col
        self.date_range = date_range
        self.group_ids = group_ids

    @classmethod
    @abstractmethod
    def from_session(cls, session, group_wise=False):
        pass


class DatetimeLocator(PredictionTransformer):
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
        super().__init__(timestamp_col, date_range)

    def fit(self, X):
        return self

    def transform(self, X):
        Xt = X.set_index(self.timestamp_col).reindex(self.date_range)
        return Xt.fillna(method='ffill').fillna(method='bfill')

    @classmethod
    def from_session(cls, session, group_wise=False):
        if group_wise:
            return _GroupWiseDatetimeLocator.from_session(session)

        timestamp_col = session.schema.get_names_for('timestamp')[0]
        date_range = session.make_date_range()
        return cls(timestamp_col, date_range)


class _GroupWiseDatetimeLocator(PredictionTransformer):
    def __init__(self, group_ids, timestamp_col, date_range, reset_index=True):
        super().__init__(timestamp_col, date_range, group_ids)
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
    def from_session(cls, session, group_wise=False):
        date_range = session.make_date_range()
        group_ids = session.schema.get_names_for('group_ids')
        timestamp = session.schema.get_names_for('timestamp')[0]
        return cls(group_ids, timestamp, date_range)


class WhatIf(PredictionTransformer):

    def __init__(
            self, date_range, timestamp_col, input_cols, value=None,
            method=None, percentage=None
    ):
        super().__init__(timestamp_col, date_range)
        self.date_range = date_range
        self.timestamp_col = timestamp_col
        self.input_cols = input_cols
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
        X.loc[mask, self.input_cols] = np.nan
        return X

    def _percentage_change(self, X, mask):
        factor = 1 + (self.percentage / 100)

        X.loc[mask, self.input_cols] *= factor
        return X

    @classmethod
    def from_session(cls, session, group_wise=False):
        if group_wise:
            return _GroupWiseWhatIf.from_session(session)
        raise NotImplementedError()


class _GroupWiseWhatIf(PredictionTransformer):
    def __init__(self, group_ids, date_range, timestamp_col, what_if_data):
        super().__init__(timestamp_col, date_range, group_ids)
        self.what_if_data = what_if_data

    def fit(self, X):
        return self

    def transform(self, X):
        groupby = X.groupby(self.group_ids, sort=False)
        apply_fn = self._make_apply_fn()
        return groupby.apply(
            apply_fn,
            group_ids_generator=(group_id for group_id in groupby.groups)
        )

    def _make_apply_fn(self):
        def apply_fn(X, group_ids_generator):
            group_id = next(iter(group_ids_generator))

            try:
                # TODO: change groupwise what if application.
                data = self.what_if_data[group_id]
            except KeyError:
                return X

            what_if = WhatIf(self.date_range, self.timestamp_col, **data)

            return what_if.fit_transform(X)

        return apply_fn

    @classmethod
    def from_session(cls, session, group_wise=False):
        what_if_data = session.what_ifs
        group_ids = session.schema.get_names_for('group_ids')
        timestamp = session.schema.get_names_for('timestamp')[0]
        _, forecast_date_range = session.make_date_range(split=True)
        return cls(group_ids, forecast_date_range, timestamp, what_if_data)
