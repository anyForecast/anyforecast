import mlflow
from sklearn.pipeline import Pipeline

from ._pandas_loader import PandasLoader
from .base_task import BaseTask
from ..ml.estimator import EstimatorCreator
from ..ml.mlflow_log import MlFlowLogger
from ..ml.preprocessor import PreprocessorCreator


class TrainingTask(BaseTask):
    """Loads, preprocess and fits timeseries data.
    """

    def __init__(self, serializer=None, task_name='training_task', bind=True):
        super().__init__(serializer, task_name, bind)

    def run(self, trainer, dataset, user, self_task):
        task_id = self.get_task_id(self_task)
        with mlflow.start_run(run_name=task_id):
            pandas_loader = PandasLoader(dataset, user)
            X, schema_resolver = pandas_loader.load_pandas(
                return_schema_resolver=True)

            # Preprocess -----
            #                |--> Pipeline.
            # Estimator -----
            preprocessor = self._create_preprocessor(
                trainer, schema_resolver)
            estimator = self._create_estimator(trainer, schema_resolver)
            pipeline = self._fit_pipeline(X, preprocessor, estimator)

            # Save model.
            logger = MlFlowLogger(pipeline, X)
            logger.log(run_name=task_id)

    def _fit_pipeline(self, X, preprocessor, estimator):
        """Collects both ``preprocessor`` and ``estimator`` into a single
        sklearn :class:`sklearn.pipeline.Pipeline` object and fits it on X.
        """
        steps = [('preprocessor', preprocessor), ('estimator', estimator)]
        pipeline = Pipeline(steps)
        pipeline.fit(X)
        return pipeline

    def _create_estimator(self, trainer, schema_resolver):
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
        args = schema_resolver.get_names_for(args_keys)
        estimator = estimator_creator.create_estimator(
            **args, time_idx='time_index')
        return estimator

    def _create_preprocessor(self, trainer, schema_resolver):
        """Creates sklearn preprocessor.

        Notice the ``preprocessor`` is itself a sklearn
        :class:`sklearn.pipeline.Pipeline` object.
        """
        args_keys = [
            'group_ids',
            'target',
            'timestamp'
        ]
        args = schema_resolver.get_names_for(args_keys)
        args['freq'] = trainer['freq']
        preprocessor_creator = PreprocessorCreator(**args)
        preprocessor = preprocessor_creator.create_preprocessor()
        return preprocessor
