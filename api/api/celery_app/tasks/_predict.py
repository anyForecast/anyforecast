from abc import ABCMeta

import pandas as pd
from sklearn.pipeline import make_pipeline
from skorch_forecasting.utils.datetime import set_date_on_freq

from ._base_task import BaseTask
from ._transformers import GroupWiseDatetimeLocator, GroupWiseWhatIf
from ..dataloaders import MlFlowLoader
from ..serialize.pandas import PandasNativeSerializer


class PredictionRun:

    def __init__(self, schema, predictor, date_range, what_ifs=None):
        self.schema = schema
        self.predictor = predictor
        self.date_range = date_range
        self.what_ifs = what_ifs
        self.mlflow_loader = MlFlowLoader()

    def get_names_for(self, keys):
        """Obtains names from schema.

        Returns
        -------
        names : list of str
        """
        return self.schema.get_names_for(keys)

    def make_pipeline(self, transformers):
        """Makes a sklearn :class:`Pipeline` using the given transformers.

        Returns
        -------
        pipeline : sklearn Pipeline
        """
        steps = []
        for trans in transformers:
            transformer = trans.from_run(self)
            steps.append(transformer)

        return make_pipeline(*steps)

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

    def split_date_range(self):
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
            end=start,
            periods=encoder_length + 1,
            inclusive='left',
            freq=freq
        )
        forecast_date_range = pd.date_range(start, end, freq=freq)

        return encoder_date_range, forecast_date_range


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(
            self, serializer=None, task_name=None, bind=False,
            transformers=None
    ):
        super().__init__(serializer, task_name, bind)
        self.transformers = transformers

    def make_run_object(self, schema, predictor, date_range, what_ifs):
        return PredictionRun(schema, predictor, date_range, what_ifs)


class GroupPredictionTask(BasePredictionTask):
    def __init__(self):
        super().__init__(serializer=PandasNativeSerializer(orient='records'))

    def run(self, data, predictor, date_range, what_ifs=None):
        # Load data and predictor.
        X, schema = data['dataframe'], data['schema']
        run = self.make_run_object(schema, predictor, date_range, what_ifs)
        predictor = run.load_predictor()

        # Pipeline for preparing input data.
        transformers = [GroupWiseDatetimeLocator]
        if what_ifs is not None:
            transformers.append(GroupWiseWhatIf)
        pipeline = run.make_pipeline(transformers)

        # Predict.
        X = pipeline.fit_transform(X)
        output = predictor.predict(X)

        return self.serialize_result(output)
