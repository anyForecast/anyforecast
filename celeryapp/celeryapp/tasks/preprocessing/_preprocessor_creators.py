from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from skorch_forecasting.preprocessing import ColumnSelector
from skorch_forecasting.preprocessing import TimeIndexEncoder

from ._steps_creator import PreprocessingStep, GroupWiseStep


class TimeseriesPreprocessorCreator:
    """Timeseries preprocessor creators.

    Aims to facilitate the construction of a timeseries preprocessor by
    combining group-wise and non group-wise transformations in a sklearn
    :class:`Pipeline`.

    Parameters
    ----------
    group_ids : list of str
        Columns identifying each timeseries in the dataframe.

    timestamp : str
        Timestamp column/feature name.

    target : str
        Target column/feature name.

    """

    def __init__(self, group_ids, timestamp, target):
        self.group_ids = group_ids
        self.timestamp = timestamp
        self.target = target

    def create_preprocessor(
            self, target_transformer=MinMaxScaler(),
            num_transformer=MinMaxScaler(), cat_encoder=OneHotEncoder(),
            freq='D'
    ):
        steps = self._make_steps(
            target_transformer,
            num_transformer,
            cat_encoder,
            freq)

        return Pipeline(steps)

    def _make_steps(
            self, target_transformer, num_transformer, cat_encoder, freq
    ):
        num = self._make_numerical_step(num_transformer)
        cat = self._make_categorical_step(cat_encoder)
        time = self._make_time_index_step(freq)
        target = self._make_target_step(target_transformer)

        return [num, cat, time, target]

    def _make_numerical_step(
            self, transformer, step_name='NumericalTransformer'
    ):
        features = ColumnSelector(
            dtype_include=['int', 'float'],
            pattern_exclude=self.target)

        return self._make_groupwise_step(
            step_name, self.group_ids, features, transformer)

    def _make_categorical_step(
            self, transformer, step_name='CategoricalEncoder'
    ):
        features = ColumnSelector(
            dtype_include=['object'],
            pattern_exclude=self.group_ids)

        return self._make_step(step_name, features, transformer)

    def _make_time_index_step(self, freq, step_name='TimeIndexEncoder'):
        encoder = TimeIndexEncoder(freq=freq)
        return self._make_step(step_name, self.timestamp, encoder)

    def _make_target_step(self, transformer, step_name='TargetTransformer'):
        return self._make_groupwise_step(
            step_name, self.group_ids, [self.target], transformer)

    def _make_groupwise_step(self, step_name, group_ids, features, transformer):
        return GroupWiseStep(
            step_name, group_ids, features, transformer).make_step()

    def _make_step(self, name, features, transformer):
        return PreprocessingStep(
            name, features, transformer).make_step()

    @classmethod
    def from_schema(cls, schema):
        kwargs = schema.get_names_for(('group_ids', 'timestamp', 'target'))
        return cls(**kwargs)
