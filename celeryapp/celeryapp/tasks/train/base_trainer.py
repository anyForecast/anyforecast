from typing import Tuple, Dict

import mlflow
import pandas as pd
import sklearn
import skorch_forecasting
from skorch_forecasting import pipelines

from .. import base, preprocessing, schemas


def make_transformer_creator(name):
    packages = {
        'sklearn': sklearn.preprocessing,
        'skorch_forecasting': skorch_forecasting.preprocessing
    }

    return TransformerCreator(packages[name])


class TransformerCreator:
    def __init__(self, package):
        self.package = package

    def create_transformer(self, name, *args, **kwargs):
        return getattr(self.package, name)(*args, **kwargs)


class BaseTrainer(base.BaseTask):
    """Base class for all Celery training tasks.

    Training tasks preprocess and fit time series data.

    Notes
    -----
    Since ´ignore_result=True´, even though the task is completed by the worker,
    the state of the task will be always PENDING.
    """

    def __init__(self):
        super().__init__(ignore_result=True)

    def get_metrics(self, estimator):
        return {}

    def get_params(self):
        pass

    def run(
            self,
            data_and_schema: Tuple[pd.DataFrame, schemas.FeaturesSchema],
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

    def fit_transform(
            self, X, preprocessor, estimator
    ) -> pipelines.PreprocessorEstimatorPipeline:
        """Transforms and fits X.

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
        pipeline : PreprocessorEstimatorPipeline
        """
        with mlflow.start_run():
            pipeline = self.make_pipeline(preprocessor, estimator)
            pipeline.fit(X)
            return pipeline

    def fit(
            self, X, preprocessor, estimator
    ) -> pipelines.PreprocessorEstimatorPipeline:
        """Fits X.

        Assumes ``X`` has been already transformed and ``preprocessor``
        is fitted.

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
        pipeline : PreprocessorEstimatorPipeline
        """
        with mlflow.start_run():
            sklearn.utils.validation.check_is_fitted(preprocessor)
            estimator.fit(X)
            return self.make_pipeline(preprocessor, estimator)

    def make_pipeline(
            self, preprocessor, estimator
    ) -> pipelines.PreprocessorEstimatorPipeline:
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
        return pipelines.PreprocessorEstimatorPipeline(preprocessor, estimator)

    def update_schema(self, schema, preprocessor):
        names = vars(schema)
        feature_names_transformer = preprocessing.FeatureNamesTransformer(
            preprocessor)
        transformed_names = feature_names_transformer.transform(names)
        schema.update_schema(transformed_names)
        return schema

    def get_transformer(self, package, name, *args, **kwargs):
        transformer_creator = make_transformer_creator(package)
        return transformer_creator.create_transformer(name, *args, **kwargs)

    def create_timeseries_preprocessor(
            self, group_ids, timestamp, target, freq, preprocessing_data=None
    ):

        if preprocessing_data is None:
            preprocessing_data = {}

        preprocessor_creator = preprocessing.TimeseriesPreprocessorCreator(
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
            transformer = self.get_transformer(package, transformer, **kwargs)
            transformers[name] = transformer

        return transformers
