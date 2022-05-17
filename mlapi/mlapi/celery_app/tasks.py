import celery
from mlflow.models.signature import infer_signature
from sklearn.pipeline import Pipeline

from .celery import app
from .client_args import ClientArgs
from .ml.datasources.parquet_loader import S3ParquetLoader
from .ml.estimator import EstimatorCreator
from .ml.logger import MlFlowLogger
from .ml.parquet_resolver import make_parquet_resolver
from .ml.preprocessor import PreprocessorCreator
from .ml.utils.data import AttrDict, get_history
from .ml.wrappers import wrap_pipeline
from .ml.utils.s3 import make_s3_path


class CreateForecasterTask(celery.Task):
    """Loads, preprocess and fits data from s3.
    """

    METRICS = ['train_loss']
    DATASETS_BUCKET = 'datasets'

    def run(self, forecaster_data, user_data):
        forecaster = AttrDict(forecaster_data)
        user = AttrDict(user_data)

        # Load data.
        resolved = self._resolve_dataset(forecaster, user)
        X = resolved['X']
        group_ids = resolved['group_ids']
        timestamp = resolved['timestamp']
        target = 'target'

        # Create both preprocessor and estimator.
        preprocessor = self._create_preprocessor(group_ids, target, timestamp)
        estimator = self._create_estimator(forecaster, group_ids)

        # Put everything inside a sklearn Pipeline and fit.
        pipeline = self._fit_pipeline(X, preprocessor, estimator)

        # Save metrics
        logger = MlFlowLogger()
        for metric in self.METRICS:
            estimator = pipeline['estimator']
            history = get_history(estimator, metric)
            logger.save_metric(name=metric, values=history)

        # Save model the model with a signature that defines the schema of
        # the model's inputs and outputs. When the model is deployed, this
        # signature will be used to validate inputs.
        wrapped_pipeline = wrap_pipeline(pipeline)
        signature = infer_signature(X, wrapped_pipeline.predict(None, X))
        logger.save_python_model(
            name='pipeline', python_model=wrapped_pipeline,
            signature=signature,
            artifacts=self._create_inference_artifacts(forecaster))

        # Log all
        logger.log_all()

    def _fit_pipeline(self, X, preprocessor, estimator):
        """Collects both `preprocessor` and `estimator` into a single
        :class:`sklearn.pipeline.Pipeline` object and fits X.
        """
        steps = [('preprocessor', preprocessor), ('estimator', estimator)]
        pipeline = Pipeline(steps)
        pipeline.fit(X)
        return pipeline

    def _create_estimator(self, forecaster, group_ids, callbacks=None):
        """Creates time series estimator.
        """
        estimator_creator = EstimatorCreator(forecaster)
        target = 'target'
        time_varying_unknown_reals = ['target']
        time_varying_known_reals = []
        static_categoricals = []

        estimator = estimator_creator.create_estimator(
            group_ids, target, time_varying_known_reals,
            time_varying_unknown_reals, static_categoricals,
            callbacks=callbacks, time_idx='time_index')
        return estimator

    def _create_preprocessor(self, group_ids, target, timestamp):
        """Creates sklearn preprocessor.
        """
        preprocessor_creator = PreprocessorCreator(
            group_ids, target, timestamp)
        preprocessor = preprocessor_creator.create_preprocessor()
        return preprocessor

    def _resolve_dataset(self, forecaster, user):
        """Calls :meth:`resolve` from :class:`TimeSeriesResolver`.

        Returns
        -------
        dict : str -> obj
        """
        client_args = {
            "s3_endpoint": user.s3_endpoint,
            "access_key": user.access_key,
            "secret_key": user.secret_key,
            'secure': False
        }
        client_args = ClientArgs(**client_args)
        parquet_loader = S3ParquetLoader(client_args)

        # Parquet loading.
        bucket_name = self.DATASETS_BUCKET
        prefix = [forecaster.dataset_group_name, forecaster.dataset_name]
        datasets = parquet_loader.load_many(bucket_name, *prefix)

        timeseries_resolver = make_parquet_resolver('timeseries_resolver')(
            **datasets)
        return timeseries_resolver.resolve()

    def _create_inference_artifacts(self, forecaster):
        if forecaster.inference_folder is not None:
            bucket_name = self.DATASETS_BUCKET
            args = [forecaster.dataset_group_name, forecaster.inference_folder]
            inference_path = make_s3_path(bucket_name, *args)
            return {"inference": inference_path}
        return None


CreateForecasterTask = app.register_task(CreateForecasterTask())
