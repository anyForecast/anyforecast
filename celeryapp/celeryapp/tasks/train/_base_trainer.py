from abc import abstractmethod, ABCMeta

import mlflow
import sklearn
import skorch_forecasting
from skorch_forecasting.model_wrappers import PreprocessorEstimatorWrapper

from .._base_task import BaseTask
from .._transformers import FeatureNamesTransformer
from ..preprocessing import TimeseriesPreprocessorCreator


class TransformersFactory:

    def __init__(self):
        self._factories = {}
        self._register_factories()

    def get_transformer(self, package, name, **kwargs):
        return self._factories[package](name)(**kwargs)

    def _register_factories(self):
        preprocessing_packages = {
            'sklearn': sklearn.preprocessing
        }

        for name, package in preprocessing_packages.items():
            self._factories[name] = self._make_factory(package)

    def _make_factory(self, package):
        """Factory for steps makers.

        Parameters
        ----------
        package : module
        """

        def factory(cls):
            return getattr(package, cls)

        return factory


class BaseTrainer(BaseTask):
    """Base class for all Celery training tasks.

    Notes
    -----
    Since ´ignore_result=True´, even though the task is completed by the worker,
    the state of the task will be always PENDING.
    """

    def __init__(self):
        super().__init__(bind=True, ignore_result=True)
        self._transformers_factory = TransformersFactory()

    def fit(self, X, preprocessor, estimator):
        print(f'skorch-forecasting version: {skorch_forecasting.__version__}')
        with mlflow.start_run():
            signature = self.infer_mlflow_signature(X)
            wrapper = self.make_wrapper(preprocessor, estimator)
            wrapper.fit(X)

            # Log model + metrics + parameters.
            metrics = self.get_metrics(wrapper.estimator)
            params = self.get_params(wrapper.estimator)
            log_to_mlflow(wrapper, signature, metrics, params)

    def get_metrics(self, estimator):
        pass

    def get_params(self, estimator):
        pass

    def transform_feature_names(self, features_names, preprocessor):
        return FeatureNamesTransformer(preprocessor).transform(features_names)

    def get_transformer(self, package, name, **kwargs):
        return self._transformers_factory.get_transformer(
            package, name, **kwargs)

    def make_wrapper(self, preprocessor, estimator):
        return PreprocessorEstimatorWrapper(preprocessor, estimator)

    def infer_mlflow_signature(self, model_input, model_output=None):
        return mlflow.models.signature.infer_signature(
            model_input, model_output)

    def create_preprocessor(
            self, group_ids, timestamp, target, freq, preprocessing_data=None
    ):

        if preprocessing_data is None:
            preprocessing_data = {}

        preprocessor_creator = TimeseriesPreprocessorCreator(
            group_ids, timestamp, target)
        transformers = self._make_transformers(preprocessing_data)
        return preprocessor_creator.create_preprocessor(
            **transformers, freq=freq)

    def _make_transformers(self, preprocessing_data):
        transformers = {}
        for name, data in preprocessing_data.items():
            package = data.get('package', 'sklearn')
            transformer = data['transformer']
            kwargs = data.get('kwargs', {})
            transformer = self._transformers_factory.get_transformer(
                package, transformer, **kwargs)
            transformers[name] = transformer

        return transformers


class BaseMlFLowLog(metaclass=ABCMeta):
    """Base abstract class for MlFlow loggers.

    All derived classed must implement the `log()` method.

    .. note::
        This class is not meant to be used directly. Use derived classes
        instead.
    """

    @abstractmethod
    def log(self):
        pass


class MetricsLog(BaseMlFLowLog):
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def log(self):
        for i, val in enumerate(self.values):
            mlflow.log_metric(key=self.name, value=val, step=i)


class ParametersLog(BaseMlFLowLog):
    def __init__(self, params):
        self.params = params

    def log(self):
        mlflow.log_params(self.params)


class ModelLog(BaseMlFLowLog):
    def __init__(self, model, conda_env, signature,
                 use_sklearn_logger=False, artifact_path='model'):
        self.model = model
        self.conda_env = conda_env
        self.signature = signature
        self.use_sklearn_logger = use_sklearn_logger
        self.artifact_path = artifact_path

    def log(self):
        logger = mlflow.sklearn if self.use_sklearn_logger else mlflow.pyfunc
        kwargs = self._create_kwargs()
        logger.log_model(**kwargs)

    def _create_kwargs(self):
        kwargs = {
            'artifact_path': self.artifact_path,
            'signature': self.signature,
            'conda_env': self.conda_env
        }
        if self.use_sklearn_logger:
            kwargs['sk_model'] = self.model
        else:
            kwargs['python_model'] = self.model
        return kwargs


class MlFlowLogger:

    def log_model(
            self, model, signature, use_sklearn_logger=True,
            artifact_path='model', conda_env=None
    ):
        model_log = ModelLog(
            model, conda_env, signature, use_sklearn_logger, artifact_path
        )
        model_log.log()

    def log_metrics(self, metrics_dict):
        for name, values in metrics_dict.items():
            MetricsLog(name, values).log()

    def log_params(self, params):
        ParametersLog(params).log()

    def _get_estimator_params(self, estimator):
        """Private function for obtaining estimator params.
        """
        allowed_types = (int, str, float, list, tuple)

        params = {}
        for k, v in estimator.get_params().items():
            if k == 'callbacks':
                for name, obj in v:
                    params[name] = obj.__class__.__name__
            elif isinstance(v, allowed_types) or v is None:
                params[k] = v
            else:
                params[k] = v.__name__
        return params


def log_to_mlflow(model, signature=None, metrics=None, parameters=None):
    """Logs to current MlFlow run.

    Parameters
    ----------
    model
    signature
    metrics
    parameters

    Returns
    -------
    """
    if metrics is None:
        metrics = {}

    if parameters is None:
        parameters = {}

    logger = MlFlowLogger()
    logger.log_model(model, signature)
    logger.log_metrics(metrics)
    logger.log_params(parameters)
