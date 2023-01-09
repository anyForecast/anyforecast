import sklearn

from ._estimator_creators import make_estimator_creator
from ._mlflow_logger import MlFlowLogger
from .._base_task import BaseTask


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
    """

    def __init__(self):
        super().__init__(bind=True)
        self._transformers_factory = TransformersFactory()

    def log_mlflow(self, model, signature, metrics, parameters):
        mlflow_logger = MlFlowLogger()
        mlflow_logger.log_model(model, signature)
        mlflow_logger.log_metrics(metrics)
        mlflow_logger.log_params(**parameters)

    def make_estimator_creator(self, name):
        return make_estimator_creator(name)

    def get_transformer(self, package, name, **kwargs):
        return self._transformers_factory.get_transformer(
            package, name, **kwargs)
