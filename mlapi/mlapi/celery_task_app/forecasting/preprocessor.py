from mooncake.preprocessing import (
    GroupTransformer,
    DataframeColumnTransformer,
    CyclicalDates
)
from mooncake.helper import column_selector
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder


class PreprocessorCreator:
    def __init__(self, group_ids, scaler=MinMaxScaler(),
                 encoder=OneHotEncoder(), cyclical_dates=True,
                 time_idx='time_idx'):
        self.group_ids = group_ids
        self.scaler = scaler
        self.encoder = encoder
        self.cyclical_dates = cyclical_dates
        self.time_idx = time_idx

    def create_preprocessor(self):
        group_trans = self._create_group_transformations()
        outer_trans = self._create_outer_transformations()

        steps = [
            ("groups", GroupTransformer(group_trans, self.group_ids)),
            #("outer", DataframeColumnTransformer(outer_trans))
        ]
        return Pipeline(steps=steps)

    def _create_group_transformations(self):
        """Transformers defined here will act group by group.
        That is, each group will be fitted to its own set of transformers.
        """
        selector = column_selector(
            dtype_include=['float', 'int'],
            pattern_exclude=self.group_ids + [self.time_idx]
        )
        group_transformations = [
            ('cont', self.scaler, selector),
        ]
        return group_transformations

    def _create_outer_transformations(self):
        timestamp_column = 'timestamp'
        selector = column_selector(
            dtype_include=['object'],
            pattern_exclude=self.group_ids
        )
        outer_transformations = [
            ('cat', self.encoder, selector),
        ]
        if self.cyclical_dates:
            outer_transformations.append(
                ('dates', CyclicalDates(), timestamp_column)
            )
        return outer_transformations
