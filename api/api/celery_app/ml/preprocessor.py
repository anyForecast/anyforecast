import numpy as np
from mooncake.helper import column_selector
from mooncake.preprocessing import (
    GroupColumnTransformer,
    ColumnTransformer,
    CyclicalDates,
    TimeIndex,
    IdentityTransformer
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder


class PreprocessorCreator:
    """Creates a preprocessor in the form of a
    :class:`sklearn.pipeline.Pipeline`.

    The preprocessor transforms the data into a numerical space suitable
    for the learning algorithm and includes the following steps:

    - Target transformations:
        Transformations defined here will act only in the target variable.

    - Group transformations:
        Transformations defined here will act group by group.

    - Column transformations:
        Transformations defined here will act on the entire data

    Parameters
    ----------
    group_ids : str or list of str.
        Group ids identifying each time series.

    target : str
        Name of target column.

    timestamp : str, default='timestamp'
        Name of datetime column.
    """

    def __init__(self, group_ids, target, timestamp='timestamp'):
        self.group_ids = group_ids
        self.target = target
        self.timestamp = timestamp

    def create_preprocessor(self, scaler=MinMaxScaler(),
                            encoder=OneHotEncoder(), cyclical_dates=True,
                            add_time_index=True):
        """Creates a preprocessor in the form of a
        :class:`sklearn.pipeline.Pipeline`.

        Parameters
        ----------
        scaler : transformer object, default=MinMaxScaler()
            Scaler for numerical columns.

        encoder : transformer object, default=OneHotEncoder()
            Encoder for categorical columns.

        cyclical_dates : bool, default=True
            Whether or not to encode dates cyclically.

        add_time_index : bool, default=True
            Whether or not to include an extra time index column.

        Returns
        -------
        preprocessor : sklearn.pipeline.Pipeline
        """
        target_transformer = self._create_target_transformer(scaler)
        group_transformer = self._create_group_transformer(
            scaler, add_time_index=add_time_index)
        column_transformer = self._create_column_transformer(
            encoder, cyclical_dates, add_time_index=add_time_index)

        steps = [
            ('target', target_transformer),
            ('column_transformer', column_transformer),
            ('group_transformer', group_transformer)
        ]
        return Pipeline(steps)

    def _create_target_transformer(self, scaler):
        """Transformers defined here will act only in the target variable.
        """
        target_transformer_triplet = [('target', scaler, [self.target])]
        target_transformer = GroupColumnTransformer(
            target_transformer_triplet, self.group_ids)
        return target_transformer

    def _create_group_transformer(self, scaler, add_time_index):
        """Transformers defined here will act group by group.
        """
        selector = self._create_column_selector(
            ['float'], to_exclude=[self.target])
        scaler_triplet = ('cont', scaler, selector)
        transformers = [scaler_triplet]

        if add_time_index:
            time_index_triplet = self._create_time_index_triplet(
                for_column_transformer=False)
            transformers.append(time_index_triplet)

        return GroupColumnTransformer(transformers, self.group_ids)

    def _create_column_transformer(self, encoder, cyclical_dates,
                                   add_time_index):
        """Transformers defined here will act on the dataframe as a whole.
        """
        selector = self._create_column_selector(['object'])
        transformers = [('cat', encoder, selector)]

        if add_time_index:
            time_index_triplet = self._create_time_index_triplet()
            transformers.append(time_index_triplet)

        if cyclical_dates:
            cyclical_dates_triplet = self._create_cyclical_dates_triplet()
            transformers.append(cyclical_dates_triplet)

        return ColumnTransformer(transformers)

    def _create_column_selector(self, dtype_include, to_exclude=None):
        if to_exclude is None:
            to_exclude = []
        pattern_exclude = self.group_ids + to_exclude
        return column_selector(dtype_include=dtype_include,
                               pattern_exclude=pattern_exclude)

    def _create_time_index_triplet(self, for_column_transformer=True):
        """When adding a time index, both ColumnTransformer and
        GroupColumnTransformer require an additional triplet.
        """
        time_index_name = 'time_index'

        if for_column_transformer:
            dtype = np.dtype('<M8[ns]')
            identity_transformer = IdentityTransformer(
                time_index_name, cast_to_object=True, dtype=dtype)
            time_index_triplet = ('identity',
                                  identity_transformer,
                                  [self.timestamp])
        else:
            time_index_triplet = ('time_index',
                                  TimeIndex(extra_timestamps=100),
                                  time_index_name)

        return time_index_triplet

    def _create_cyclical_dates_triplet(self):
        if self.timestamp is None:
            raise ValueError(
                '`timestamp` init param cannot be None for '
                '`cyclical_dates=True` .'
            )
        return 'cyclical_dates', CyclicalDates(), self.timestamp
