from abc import ABCMeta

import numpy as np
import pandas as pd
from pyrm.price_opt import ResponseFunctionEstimator
from sklearn.pipeline import make_pipeline
from skorch_forecasting.utils.datetime import set_date_on_freq

from ._base_task import BaseTask
from ._partial_predictor import PartialPredictor
from ._transformers import DatetimeLocator, WhatIf
from ..dataloaders import MlFlowLoader
from ..serialize.pandas import PandasNativeSerializer


class PredictionSession:

    def __init__(self, predictor, date_range, what_ifs=None):
        self.predictor = predictor
        self.date_range = date_range
        self.what_ifs = what_ifs
        self.mlflow_loader = MlFlowLoader()

    def make_predictor(self):
        if not hasattr(self, 'schema'):
            raise
        return SessionPredictor(self)

    def set_schema(self, schema):
        setattr(self, 'schema', schema)

    def get_model_params(self):
        """Retrieves model params from MlFlow.

        Returns
        -------
        model_params : dict
        """
        model_name = self.predictor['model_name']
        latest_model_version = self.mlflow_loader.get_latest_model_version(
            model_name)
        model_params = self.mlflow_loader.get_run_id_params(
            latest_model_version.run_id)
        return model_params

    def load_predictor(self, stage='production'):
        """Loads ready-to-predict model from MlFlow.

        Returns
        -------
        model : MlFlow model
        """
        model_name = self.predictor['model_name']
        return self.mlflow_loader.load_predictor(model_name, stage)

    def make_date_range(self, split=False):
        """Splits date range.

        Returns
        -------
        date_range : pd.DateTimeIndex
        """
        model_params = self.get_model_params()
        encoder_length = int(model_params['max_encoder_length'])
        freq = model_params['freq']

        # Set dates to given frequency
        start = set_date_on_freq(self.date_range['start'], freq)
        end = set_date_on_freq(self.date_range['end'], freq)

        # ``encoder_date_range`` refers to the collection of dates prior
        # to the forecasting dates needed for sequential models.
        encoder_date_range = pd.date_range(
            end=start, periods=encoder_length + 1, inclusive='left', freq=freq)

        # ``forecast_date_range`` refers to the actual forecasting dates.
        forecast_date_range = pd.date_range(start, end, freq=freq)

        if split:
            return encoder_date_range, forecast_date_range
        return encoder_date_range.union(forecast_date_range)


class SessionPredictor:
    def __init__(self, session):
        self.session = session

    def transform(self, X):
        transformers = [DatetimeLocator]
        if self.session.what_ifs is not None:
            transformers.append(WhatIf)

        steps = [
            trans.from_session(self.session, group_wise=True)
            for trans in transformers
        ]

        pipeline = make_pipeline(*steps)
        return pipeline.fit_transform(X)

    def predict(self, X):
        predictor = self.session.load_predictor()
        return predictor.predict(X)

    def transform_predict(self, X):
        return self.predict(self.transform(X))

    def to_partial(self, X, input_cols):
        X = self.transform(X)
        timestamp_col = self.session.schema.get_names_for('timestamp')[0]
        predictor = self.session.load_predictor()
        date_range = self.session.make_date_range(split=True)[1]
        return PartialPredictor(X, timestamp_col, input_cols, predictor,
                                date_range)


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(self, serializer=None, task_name=None, bind=False):
        super().__init__(serializer, task_name, bind)

    def make_session(self, predictor, date_range, what_ifs=None):
        return PredictionSession(predictor, date_range, what_ifs)


class GroupPredictionTask(BasePredictionTask):
    def __init__(self):
        super().__init__(serializer=PandasNativeSerializer(orient='records'))

    def run(self, data, predictor, date_range, what_ifs=None):
        X, schema = data['X'], data['schema']
        session = self.make_session(predictor, date_range, what_ifs)
        session.set_schema(schema)
        predictor = session.make_predictor()
        output = predictor.transform_predict(X)


        output = pd.pivot(output, index='timestamp', columns='PRODUCT_ID', values='target').reset_index()
        print(output)

        return self.serialize_result(output)


class ResponseFunctionEstimationTask(BasePredictionTask):
    DEFAULT_ESTIMATION_POINTS = 15

    def __init__(self, serializer=None, task_name=None, bind=False):
        super().__init__(serializer, task_name, bind)

    def run(self, data, predictor, date_range, input_col):
        X, schema = data['X'], data['schema']
        session = self.make_session(predictor, date_range)
        session.set_schema(schema)

        predictor = session.make_predictor()
        partial_predictor = predictor.to_partial(X, [input_col])
        X = self._create_partial_inputs(X, input_col)

        target = schema.get_names_for('target')[0]
        estimator = self._fit_response_function_estimator(
            X, partial_predictor, target)

        y = estimator.predict(X)
        return {'X': X, 'y': y}

    def _create_partial_inputs(self, X, input_col, n=None):
        if n is None:
            n = self.DEFAULT_ESTIMATION_POINTS

        start = X[input_col].min()
        stop = X[input_col].max()
        return np.linspace(start, stop, n)

    def _fit_response_function_estimator(
            self, X, partial_predictor, target
    ):
        # Predict for each input.
        # Notice ``y`` contains the aggregated output for each element in
        # ``X``.
        y = [partial_predictor.predict(x)[target].sum() for x in X]

        # Actual estimation of response function.
        estimator = ResponseFunctionEstimator('linear')
        X = X.reshape(-1, 1)
        estimator.fit(X, y)

        return estimator