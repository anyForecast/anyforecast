from sklearn.base import BaseEstimator, TransformerMixin


class WhatIfTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, feature, datetime, start, end, value):
        self.feature = feature
        self.datetime = datetime
        self.value = value
        self.start = start
        self.end = end

    def fit(self):
        return self

    def transform(self, X):
        date_mask = X[self.datetime].between(self.start, self.end)
        X.loc[date_mask, self.feature] = self.value
        return X

    def _more_tags(self):
        return {'stateless': True}
