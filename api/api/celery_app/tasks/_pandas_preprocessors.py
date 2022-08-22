import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


def preprocess_pandas(pandas, category_as_object, combine_date_features=False,
                      copy=False):
    kwargs = {
        'category_as_object': category_as_object,
        'combine_date_features': combine_date_features
    }
    preprocessor = PandasPreprocessor(**kwargs)
    return preprocessor.preprocess(pandas, copy=copy)


class PandasPreprocessor:

    def __init__(self, category_as_object=True, combine_date_features=True):
        self.category_as_object = category_as_object
        self.combine_date_features = combine_date_features

    def _make_preprocessor(self, name, copy):
        preprocessors = {
            'category_as_object': CategoryAsObject,
            'combine_date_features': CombineDateFeatures
        }
        return preprocessors[name](copy)

    def _collect_preprocessors(self, copy):
        preprocessors = []
        for preprocessor_name, true_or_false in vars(self).items():
            if true_or_false:
                preprocessor = self._make_preprocessor(preprocessor_name, copy)
                preprocessors.append(preprocessor)
        return preprocessors

    def preprocess(self, X, copy=True):
        preprocessors = self._collect_preprocessors(copy)
        for p in preprocessors:
            X = p.fit_transform(X)
        return X


class BasePreprocessor(BaseEstimator, TransformerMixin):
    """Base abstract class for Pandas preprocessors.

    Parameters
    ----------
    copy : bool, default=False
    """

    def __init__(self, copy=False):
        self.copy = copy


class CombineDateFeatures(BasePreprocessor):
    def __init__(self, copy=False):
        super().__init__(copy)

    def fit(self, X):
        return self

    def transform(self, X):
        if self.copy:
            X = X.copy()

        names = ['year', 'month', 'day']
        X['date'] = pd.to_datetime(X[names])

        # Drop original split dates.
        X.drop(labels=names, axis='columns', inplace=True)

        return X

    def _more_tags(self):
        return {'stateless': True}


class CategoryAsObject(BasePreprocessor):
    def __init__(self, inplace=False):
        super().__init__(inplace)
        self.inplace = inplace

    def fit(self, X):
        return self

    def transform(self, X):
        if self.copy:
            X = X.copy()

        X.loc[:, X.dtypes == 'category'] = X.select_dtypes(
            ['category']).apply(lambda x: x.astype('object'))

        return X

    def _more_tags(self):
        return {'stateless': True}
