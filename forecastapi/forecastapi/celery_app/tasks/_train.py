import mlflow
from sklearn.preprocessing import MinMaxScaler
from skorch_forecasting.model_wrappers import PreprocessorEstimatorWrapper
from skorch_forecasting.preprocessing import (
    PreprocessorCreator,
    ColumnDuplicator,
    CyclicalDatesEncoder
)

from ._base_task import BaseTask
from ..ml import EstimatorCreator, MlFlowLogger, FeaturesSegmenter


class TrainTask(BaseTask):
    """Loads, preprocess and fits timeseries data.
    """

    def __init__(self):
        super().__init__(bind=True)

    def run(self, bind, data, trainer):
        X, schema = data['X'], data['schema']

        with mlflow.start_run(run_name=self.get_task_id(bind)):
            preprocessor = self.create_preprocessor(trainer, schema)

            # Segment features.
            preprocessor.fit(X)
            segmentation = self._segment_features(preprocessor, schema)

            # Init model and fit.
            # The model wraps both ``preprocessor`` and ``estimator`` into a
            # single sklearn :class:`Pipeline`. Hence, the user can input
            # non transformed data and the model itself will take care of it.
            estimator = self.create_estimator(trainer, segmentation, schema)
            model = PreprocessorEstimatorWrapper(
                estimator=estimator,
                preprocessor=preprocessor,
                inverse_transform_steps=['target_step', 'time_index_encoder']
            )
            model.fit(X)

            # Log metrics and model itself to MlFlow.
            mlflow_logger = MlFlowLogger(model, use_sklearn_logger=True)
            mlflow_logger.log(**trainer)

    def create_estimator(self, trainer, segmentation, schema):
        """Creates time series estimator.
        """
        time_idx = schema.timestamp.names[0]
        estimator_creator = EstimatorCreator(trainer)
        estimator = estimator_creator.create_estimator(
            **segmentation, time_idx=time_idx)
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

        # :class:`ColumnDuplicator` step.
        timestamp_col = schema.timestamp.names[0]
        duplicated_col_name = timestamp_col + '_duplicated'
        col_duplicator = ColumnDuplicator(out_feature=duplicated_col_name)
        col_duplicator_tuple = [(col_duplicator, timestamp_col)]
        col_duplicator_step = preprocessor_creator.make_step(
            'col_duplicator', col_duplicator_tuple)

        # :class:`TimeIndexEncoding` step.
        freq = trainer['freq']
        time_index_step = preprocessor_creator.make_time_index_encoding_step(
            timestamp_col, extra_timestamps=100, freq=freq)

        # :class:`CyclicalDatesEncoder` step
        cyclical_dates_enc = CyclicalDatesEncoder(month=False)
        cyclical_dates_enc_tuple = [(cyclical_dates_enc, duplicated_col_name)]
        cyclical_dates_step = preprocessor_creator.make_step(
            'cyclical_dates', cyclical_dates_enc_tuple)

        # Target and numerical transformation steps.
        target_step = preprocessor_creator.make_target_step(
            transformer=MinMaxScaler(clip=True)
        )
        numerical_step = preprocessor_creator.make_numerical_step(
            transformer=MinMaxScaler(clip=True),
            to_exclude=[timestamp_col]
        )

        steps = [
            col_duplicator_step,
            time_index_step,
            cyclical_dates_step,
            target_step,
            numerical_step
        ]

        return preprocessor_creator.create_preprocessor(steps)

    def _segment_features(self, preprocessor, schema):
        return FeaturesSegmenter(
            preprocessor,
            schema,
            # TODO: In order to avoid this ugly looking param, make a Step
            #  class with an attribute telling whether or not the step
            #  should be ignored.
            steps_to_ignore=['col_duplicator', 'time_index_encoder']
        ).segment()
