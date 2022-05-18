import celery
import mlflow
from sklearn.pipeline import Pipeline

from .celery import app
from .client_args import ClientArgs
from .ml.datasources import S3ParquetLoader
from .ml.estimator import EstimatorCreator
from .ml.parquet_resolver import make_parquet_resolver
from .ml.preprocessor import PreprocessorCreator
from .ml.mlflow_log import MlFlowLogger

DATASETS_BUCKET = 'datasets'


class _AttrDict(dict):
    """Auxiliary class for accessing dictionary keys like attributes.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def _create_client_args(user):
    """Private function for creating a :class:`ClientArgs` instance.
    """
    client_args = {
        "s3_endpoint": user.s3_endpoint,
        "access_key": user.access_key,
        "secret_key": user.secret_key,
        'secure': False
    }
    return ClientArgs(**client_args)


def _resolve_parquet_datasets(dataset, user):
    """Private function for resolving parquet datasets.
    """
    client_args = _create_client_args(user)

    # Load all available parquets.
    parquet_loader = S3ParquetLoader(client_args)
    prefix = [dataset.dataset_group_name, dataset.dataset_name]
    datasets = parquet_loader.load_many(DATASETS_BUCKET, *prefix)

    # Resolve parquets.
    resolver = make_parquet_resolver('timeseries_resolver', **datasets)
    return resolver.resolve()


class CreateForecasterTask(celery.Task):
    """Loads, preprocess and fits timeseries data.
    """

    def run(self, forecaster, dataset, user):
        forecaster, dataset, user = map(_AttrDict, (forecaster, dataset, user))

        with mlflow.start_run(run_name=forecaster.task_name):
            resolved = _resolve_parquet_datasets(dataset, user)
            X = resolved['X']
            group_ids = resolved['group_ids']
            timestamp = resolved['timestamp']
            target = 'target'

            preprocessor = self._create_preprocessor(
                group_ids, target, timestamp)
            estimator = self._create_estimator(forecaster, group_ids)
            pipeline = self._fit_pipeline(X, preprocessor, estimator)

            logger = MlFlowLogger(pipeline, X)
            logger.log(run_name=forecaster.task_name)

    def _fit_pipeline(self, X, preprocessor, estimator):
        """Collects both `preprocessor` and `estimator` into a single
        sklearn pipeline object and fits X.
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


CreateForecasterTask = app.register_task(CreateForecasterTask())


class PredictTask(celery.Task):

    def run(self, model, dataset, user):
        model, dataset, user = map(_AttrDict, (model, dataset, user))

        model_input = self._load_model_input(dataset, user)
        model = self._load_model(model)
        model_output = model.predict(model_input)
        return model_output.to_json()

    def _load_model_input(self, dataset, user):
        resolved = _resolve_parquet_datasets(dataset, user)
        return resolved['X']

    def _load_model(self, model):
        return mlflow.pyfunc.load_model(f"models:/{model.name}/production")


PredictTask = app.register_task(PredictTask())
