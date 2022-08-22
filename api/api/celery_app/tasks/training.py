import mlflow
from mooncake.model_wrappers import PreprocessorEstimatorWrapper
from mooncake.preprocessing import (
    PreprocessorCreator,
    ColumnDuplicator,
    CyclicalDatesEncoder
)

from .base_task import BaseTask
from ..ml.estimator import EstimatorCreator
from ..ml.mlflow_log import MlFlowLogger


class TrainingTask(BaseTask):
    """Loads, preprocess and fits timeseries data.
    """

    def __init__(self, serializer=None, task_name='TrainingTask', bind=True):
        super().__init__(serializer, task_name, bind)

    def run(self, task_id, trainer, dataset, user):
        with mlflow.start_run(run_name=task_id):
            X, schema = self.load_pandas_and_schema(dataset, user)
            preprocessor = self.create_preprocessor(trainer, schema)
            estimator = self.create_estimator(trainer, schema)
            model = PreprocessorEstimatorWrapper(
                estimator=estimator,
                preprocessor=preprocessor,
                inverse_transform_steps=[
                    'time_index_encoder',
                    'target_transformer'
                ]
            )
            model.fit(X)

            mlflow_logger = MlFlowLogger(model, use_sklearn_logger=True)
            mlflow_logger.log()

    def create_estimator(self, trainer, schema):
        """Creates time series estimator.
        """
        args_keys = [
            'group_ids',
            'target',
            'time_varying_known',
            'time_varying_unknown',
            'static_categoricals'
        ]
        estimator_creator = EstimatorCreator(trainer)
        args = schema.get_names_for(args_keys)
        estimator = estimator_creator.create_estimator(
            **args, time_idx='time_index')
        return estimator

    def create_preprocessor(self, trainer, schema):
        """Creates sklearn preprocessor.

        Notice the ``preprocessor`` is itself a sklearn
        :class:`Pipeline` object.
        """
        # Create :class:`PreprocessorCreator` instance.
        kwargs = schema.get_names_for(('group_ids', 'target'))
        kwargs['target'] = kwargs['target'][0]
        preprocessor_creator = PreprocessorCreator(**kwargs)

        # Default steps.
        default_steps = preprocessor_creator.make_default_steps()

        # :class:`ColumnDuplicator` step.
        timestamp_col = schema.timestamp.names[0]
        time_index_col = 'time_index'
        col_duplicator = ColumnDuplicator(time_index_col)
        col_duplicator_tuple = [(col_duplicator, timestamp_col)]
        col_duplicator_step = preprocessor_creator.make_step(
            'col_duplicator', col_duplicator_tuple)

        # :class:`TimeIndexEncoding` step.
        freq = trainer['freq']
        time_index_step = preprocessor_creator.make_time_index_encoding_step(
            time_index_col, extra_timestamps=100, freq=freq)

        # :class:`CyclicalDatesEncoder` step
        cyclical_dates_enc = CyclicalDatesEncoder()
        cyclical_dates_enc_tuple = [(cyclical_dates_enc, timestamp_col)]
        cyclical_dates_step = preprocessor_creator.make_step(
            'cyclical_dates', cyclical_dates_enc_tuple)

        steps = default_steps + [col_duplicator_step, time_index_step,
                                 cyclical_dates_step]

        return preprocessor_creator.create_preprocessor(steps)
