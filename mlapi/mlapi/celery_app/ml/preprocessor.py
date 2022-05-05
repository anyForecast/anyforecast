from mooncake.helper import column_selector
from mooncake.preprocessing import (
    GroupColumnTransformer,
    ColumnTransformer,
    CyclicalDates,
    TimeIndex
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder


class PreprocessorCreator:
    """
    Parameters
    ----------
    group_ids = str or list of str.
        Group ids identifying each time series.

    timestamp : str, default=None
        Name of datetime column.
    """

    def __init__(self, group_ids, timestamp='timestamp'):
        self.group_ids = group_ids
        self.timestamp = timestamp

    def create_preprocessor(self, scaler=MinMaxScaler(),
                            encoder=OneHotEncoder(), cyclical_dates=True,
                            add_time_index=True, to_exclude=None):
        """Creates a preprocessor in the form of a
        :class:`sklearn.pipeline.Pipeline`.

        Parameters
        ----------
        scaler : sklearn transformer, default=`MinMaxScaler()`
            Scaler for numerical columns.

        encoder : sklearn encoder, default=`OneHotEncoder()`
            Encoder for categorical columns.

        cyclical_dates : bool, default=True
            Whether or not to encode dates cyclically.

        to_exclude : list, default=None
            Columns to exclude from preprocessing.

        add_time_index : bool, default=True
            If True, the returned preprocessor when transforming will expect
            a datetime column named "time_index" that will turn dates into
            an index.
        """
        if to_exclude is None:
            to_exclude = []

        group_trans = self._create_group_transformer(
            scaler, to_exclude, add_time_index)
        column_trans = self._create_column_transformer(
            encoder, cyclical_dates, to_exclude)

        steps = [
            ("groups", group_trans),
            ("non_group", column_trans)
        ]
        return Pipeline(steps=steps)

    def _create_group_transformer(self, scaler, to_exclude, add_time_index):
        """Transformers defined here will act group by group.
        """
        transformers = []
        if add_time_index:
            time_index_triplet = ('time_index', TimeIndex(), 'time_index')
            transformers.append(time_index_triplet)

        selector = self._create_column_selector(['float'], to_exclude)
        scaler_triplet = ('cont', scaler, selector)
        transformers.append(scaler_triplet)

        return GroupColumnTransformer(transformers, self.group_ids)

    def _create_column_transformer(self, encoder, cyclical_dates, to_exclude):
        """Transformers defined here will act on the dataframe as a whole.
        """
        selector = self._create_column_selector(['object'], to_exclude)
        transformers = [('cat', encoder, selector)]

        if cyclical_dates:
            if self.timestamp is None:
                raise ValueError(
                    '`timestamp` init param cannot be None for '
                    '`cyclical_dates=True` .'
                )
            transformers.append(('cyclical_dates', CyclicalDates(),
                                 self.timestamp))
        return ColumnTransformer(transformers)

    def _create_column_selector(self, dtype_include, to_exclude):
        pattern_exclude = self.group_ids + to_exclude
        return column_selector(dtype_include=dtype_include,
                               pattern_exclude=pattern_exclude)
