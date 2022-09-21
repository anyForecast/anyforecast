from sklearn.base import BaseEstimator, TransformerMixin

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