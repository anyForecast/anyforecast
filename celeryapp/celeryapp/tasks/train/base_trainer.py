from typing import Tuple, Dict, Union

import mlflow
import pandas as pd
import sklearn
#from skorch_forecasting import pipelines

from ..base import BaseTask
from ..preprocessing.creators import TimeseriesPreprocessorCreator
from ..preprocessing.transformers import FeatureNamesTransformer
from ...dataloaders import FeaturesSchema

Estimator = sklearn.base.BaseEstimator
Transformer = Union[sklearn.base.BaseEstimator, sklearn.base.TransformerMixin]


class BaseTrainer(BaseTask):
    """Base class for all Celery training tasks.

    Training tasks preprocess and fit time series data.

    Notes
    -----
    Since ´ignore_result=True´, even though the task is completed by the worker,
    the state of the task will be always PENDING.
    """

    def __init__(self, estimator_creator):
        super().__init__(ignore_result=True)
        self.estimator_creator = estimator_creator
        self._transformers_factory = TransformersFactory()

    def get_metrics(self, estimator) -> Dict:
        pass

    def get_params(self, estimator) -> Dict:
        pass

    def run(
            self,
            data_and_schema: Tuple[pd.DataFrame, FeaturesSchema],
            trainer: Dict
    ) -> None:
        """Runs training task.

        Parameters
        ----------
        data_and_schema : tuple
            Tuple of (pd.DataFrame, FeatureSchema)

        trainer : dict
            Training settings.
        """
        raise NotImplementedError()

    def fit(self, X, preprocessor: Transformer,
            estimator: Estimator): #-> pipelines.PreprocessorEstimatorPipeline:
        """Preprocess and fits X.

        Parameters
        ----------
        X : pd.DataFrame
            Training data.

        preprocessor : Transformer
            Fitted transformer (implementing `fit`/`transform`).

        estimator : Estimator
            Fitted estimator (implementing `fit`/`predict`).

        Returns
        -------
        Model : PreprocessorEstimatorPipeline
        """
        with mlflow.start_run():
            pipeline = self.make_pipeline(preprocessor, estimator)
            pipeline.fit(X)
            return pipeline

    def make_pipeline(
            self,
            preprocessor: Transformer,
            estimator: Estimator
    ): # -> pipelines.PreprocessorEstimatorPipeline:
        """Creates pipeline.

        Parameters
        ----------
        preprocessor : Transformer
            Fitted transformer (implementing `fit`/`transform`).

        estimator : Estimator
            Fitted estimator (implementing `fit`/`predict`).

        Returns
        -------
        pipeline : PreprocessorEstimatorPipeline

        """
        return #pipelines.PreprocessorEstimatorPipeline(preprocessor, estimator)

    def create_estimator_from_schema(self, schema: FeaturesSchema, *args,
                                     **kwargs) -> Estimator:

        """Creates Estimator from features schema.

        Parameters
        ----------
        schema : FeaturesSchema

        args : Positional args
             Estimator creator positional args.

        kwargs : Key word args
            Estimator creator key word args.

        Returns
        -------
        estimator : Estimator
        """
        estimator_creator = self.estimator_creator.from_schema(schema)
        return estimator_creator.create_estimator(*args, **kwargs)

    def update_schema(self, schema, preprocessor):
        names = vars(schema)
        feature_names_transformer = FeatureNamesTransformer(preprocessor)
        transformed_names = feature_names_transformer.transform(names)
        schema.update_schema(transformed_names)
        return schema

    def get_transformer(self, package, name, **kwargs):
        return self._transformers_factory.get_transformer(
            package, name, **kwargs)

    def create_timeseries_preprocessor(
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
