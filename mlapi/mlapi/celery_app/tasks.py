import celery
from sklearn.pipeline import Pipeline

from .celery import app
from .client_args import ClientArgs
from .ml.datasources.s3 import ParquetLoader
from .ml.estimator import EstimatorCreator
from .ml.logger import MlFlowLogger
from .ml.parquet_resolver import TimeSeriesResolver
from .ml.preprocessor import PreprocessorCreator
from .ml.utils.data import AttrDict, get_history
from .ml.utils.pandas import duplicate_pandas_column


class CreateForecasterTask(celery.Task):
    """Loads, preprocess and fits data from s3.
    """

    METRICS = ['train_loss']

    def run(self, forecaster_data, user_data):
        forecaster = AttrDict(forecaster_data)
        user = AttrDict(user_data)

        # Load data.
        resolved = self._resolve_dataset(forecaster, user)
        X = resolved['X']
        group_ids = resolved['group_ids']
        timestamp = resolved['timestamp']

        # Create both preprocessor and estimator.
        preprocessor = self._create_preprocessor(group_ids, timestamp)
        estimator = self._create_estimator(forecaster, group_ids)

        # `preprocessor` is expecting two datetime columns: one for the time
        # index and one for the cyclical dates. So, the timestamp column
        # is duplicated.
        duplicate_pandas_column(X, column=timestamp, new_name='time_index',
                                inplace=True)

        # Put everything inside a sklearn Pipeline and fit.
        pipeline = self._fit_pipeline(X, preprocessor, estimator)

        # Save metrics
        logger = MlFlowLogger()
        for metric in self.METRICS:
            estimator = pipeline['estimator']
            history = get_history(estimator, metric)
            logger.save_metric(name=metric, values=history)

        # Save model
        logger.save_model(name='pipeline', model=pipeline)

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

    def _create_preprocessor(self, group_ids, timestamp):
        """Creates sklearn preprocessor.
        """
        preprocessor_creator = PreprocessorCreator(group_ids, timestamp)
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
        parquet_loader = ParquetLoader(forecaster.dataset_name, client_args)
        datasets = parquet_loader.load_all()
        timeseries_resolver = TimeSeriesResolver(**datasets)
        return timeseries_resolver.resolve()


CreateForecasterTask = app.register_task(CreateForecasterTask())
