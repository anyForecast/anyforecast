import mlflow
import sklearn
from skorch_forecasting.model_wrappers import PreprocessorEstimatorWrapper

from ._mlflow import log_to_mlflow
from ..preprocessing import TimeseriesPreprocessorCreator
from .._base_task import BaseTask
from .._transformers import FeatureNamesTransformer


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

    def fit(self, X, preprocessor, estimator):
        with mlflow.start_run():
            signature = self.infer_mlflow_signature(X)
            wrapper = self.make_wrapper(preprocessor, estimator)
            wrapper.fit(X)

            # Log model + metrics + parameters.
            metrics = self.get_metrics(wrapper.estimator)
            params = self.get_params(wrapper.estimator)
            log_to_mlflow(wrapper, metrics, params, signature)

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
