from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from skorch_forecasting import preprocessing


class StepsCreator:
    """
    """

    def make_step(self, step_name, transformers):
        transformer = preprocessing.PandasColumnTransformer(transformers)
        return self._make_step(step_name, transformer)

    def make_groupwise_step(self, step_name, transformers, group_ids):
        transformer = preprocessing.GroupWiseColumnTransformer(
            transformers, group_ids)
        return self._make_step(step_name, transformer)

    def _make_step(self, step_name, transformer):
        """Private function for making a single sklearn :class:`Pipeline` step.
        """
        return step_name, transformer


class PreprocessingStep:
    def __init__(self, name, features, transformer):
        self.name = name
        self.features = features
        self.transformer = transformer
        self._steps_creator = StepsCreator()

    def _make_transformers(self):
        return [(self.transformer, self.features)]

    def make_step(self):
        transformers = self._make_transformers()
        return self._steps_creator.make_step(self.name, transformers)


class GroupWiseStep(PreprocessingStep):
    def __init__(self, name, group_ids, features, transformer):
        super().__init__(name, features, transformer)
        self.group_ids = group_ids

    def make_step(self):
        transformers = self._make_transformers()
        return self._steps_creator.make_groupwise_step(
            self.name, transformers, self.group_ids)


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
        num_column_selector = preprocessing.make_column_selector(
            dtype_include=['int', 'float'],
            pattern_exclude=self.target)

        return self._make_groupwise_step(
            step_name, self.group_ids, num_column_selector, transformer)

    def _make_categorical_step(
            self, transformer, step_name='CategoricalEncoder'
    ):
        cat_column_selector = preprocessing.make_column_selector(
            dtype_include=['object'],
            pattern_exclude=self.group_ids)

        return self._make_step(step_name, cat_column_selector, transformer)

    def _make_time_index_step(self, freq, step_name='TimeIndexEncoder'):
        encoder = preprocessing.TimeIndexEncoder(freq=freq)
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
