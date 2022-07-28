import mlflow
from sklearn.pipeline import Pipeline

from ._client_args import create_client_args
from ..celery import app
from ..ml.estimator import EstimatorCreator
from ..ml.loaders import DataLoader
from ..ml.mlflow_log import MlFlowLogger
from ..ml.preprocessor import PreprocessorCreator
from ..ml.schema_resolver import SchemaResolver


class CreateForecasterTask:
    """Loads, preprocess and fits timeseries data.
    """

    def run(self, forecaster, dataset, user):
        with mlflow.start_run(run_name=forecaster['task_name']):
            # Create :class:`DataLoader` object.
            client_args = create_client_args(user)
            loader = DataLoader(client_args)

            # Create :class:`SchemaResolver` object.
            schema_data = loader.load_schema(**dataset)
            schema_resolver = SchemaResolver(schema_data)

            # Load parquet
            arrow_schema = schema_resolver.to_arrow()
            parquet = loader.load_parquet(**dataset, schema=arrow_schema)

            # Convert to parquet to pandas :class:`DataFrame`.
            X = parquet.read_pandas().to_pandas()

            # Preprocess -----
            #                |--> Pipeline.
            # Estimator -----
            preprocessor = self._create_preprocessor(
                forecaster, schema_resolver)
            estimator = self._create_estimator(forecaster, schema_resolver)
            pipeline = self._fit_pipeline(X, preprocessor, estimator)

            # Save model.
            logger = MlFlowLogger(pipeline, X)
            logger.log(run_name=forecaster['task_name'])

    def _fit_pipeline(self, X, preprocessor, estimator):
        """Collects both ``preprocessor`` and ``estimator`` into a single
        sklearn :class:`sklearn.pipeline.Pipeline` object and fits it on X.
        """
        steps = [('preprocessor', preprocessor), ('estimator', estimator)]
        pipeline = Pipeline(steps)
        pipeline.fit(X)
        return pipeline

    def _create_estimator(self, forecaster, schema_resolver):
        """Creates time series estimator.
        """
        args_keys = [
            'group_ids',
            'target',
            'time_varying_known',
            'time_varying_unknown',
            'static_categoricals'
        ]
        estimator_creator = EstimatorCreator(forecaster)
        args = schema_resolver.get_names_for(args_keys)
        estimator = estimator_creator.create_estimator(
            **args, time_idx='time_index')
        return estimator

    def _create_preprocessor(self, forecaster, schema_resolver):
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
        args['freq'] = forecaster['freq']
        preprocessor_creator = PreprocessorCreator(**args)
        preprocessor = preprocessor_creator.create_preprocessor()
        return preprocessor


@app.task(name='create_forecaster')
def create_forecaster_task(forecaster, dataset, user):
    CreateForecasterTask().run(forecaster, dataset, user)
