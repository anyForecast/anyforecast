from skorch_forecasting.preprocessing import (
    PandasColumnTransformer,
    GroupWiseColumnTransformer
)


class _StepsCreator:
    """
    """

    def make_step(self, step_name, transformers):
        transformer = PandasColumnTransformer(transformers)
        return self._make_step(step_name, transformer)

    def make_groupwise_step(self, step_name, transformers, group_ids):
        transformer = GroupWiseColumnTransformer(transformers, group_ids)
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
        self._steps_creator = _StepsCreator()

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
