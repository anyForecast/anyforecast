from abc import ABCMeta

import pandas as pd
from sklearn.pipeline import make_pipeline
from skorch_forecasting.utils.datetime import set_date_on_freq

from ._base_task import BaseTask
from ._transformers import make_transformer_from_run
from ..dataloaders import MlFlowLoader
from ..serialize.pandas import PandasNativeSerializer


def _make_partition_filter(group_params):
    """Private function for making a partition filter.

    The returned function in given to the dataloader for loading only the
    requested partitions.

    Returns
    -------
    filter_function : callable
    """
    # Collect group_ids from ``group_params``.
    group_ids = []
    for param in group_params:
        group_id = param['group_id']
        group_ids.append(group_id)

    if not group_ids:
        return None

    def filter_function(partition):
        return partition in group_ids

    return filter_function


class PredictionRun:

    def __init__(self, schema, predictor, dataset, user, date_range,
                 group_params):
        self.schema = schema
        self.predictor = predictor
        self.dataset = dataset
        self.user = user
        self.date_range = date_range
        self.group_params_resolver = GroupParamsResolver(group_params)
        self.mlflow_loader = MlFlowLoader()

    def get_names_for(self, keys):
        """Obtains names from schema.

        Returns
        -------
        names : list of str
        """
        return self.schema.get_names_for(keys)

    def make_pipeline(self, transformers_names):
        """Makes a sklearn :class:`Pipeline` using the given transformers.

        Returns
        -------
        pipeline : sklearn Pipeline
        """
        steps = []
        for name in transformers_names:
            transformer = make_transformer_from_run(name, self)
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


class GroupParamsResolver:
    def __init__(self, group_params_data):
        self.group_params_data = group_params_data

    def get_what_if_data(self):
        """Obtains what_if data from ``prediction_params``.

        Returns
        -------
        what_if_data : dict, str -> dict
        """
        what_if_data = {}
        for group_data in self.group_params_data:
            # Obtain group id values.
            group_id = list(group_data['group_id'].values())
            group_id = group_id[0] if len(group_id) == 1 else tuple(group_id)

            if 'what_if' in group_data:
                what_if_value = group_data['what_if']
                if what_if_value is not None:
                    what_if_data[group_id] = what_if_value

        return what_if_data


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(self, serializer=PandasNativeSerializer(), task_name=None,
                 bind=False):
        super().__init__(serializer, task_name, bind)

    def make_run_object(self, schema, predictor, dataset, user, date_range,
                        group_params):
        return PredictionRun(schema, predictor, dataset, user, date_range,
                             group_params)

    def load_pandas_and_schema_for_prediction(
            self, dataset, user, group_params
    ):
        filter_fn = _make_partition_filter(group_params)
        return self.load_pandas_and_schema(dataset, user, filter_fn)


class GroupPrediction(BasePredictionTask):
    def __init__(self):
        super().__init__(
            serializer=PandasNativeSerializer(orient='records'),
            task_name='GroupPrediction'
        )
        self.transformers = ['datetime_locator', 'what_if']

    def run(self, predictor, dataset, user, date_range, group_params,
            return_truth):
        X, schema = self.load_pandas_and_schema_for_prediction(
            dataset, user, group_params)
        run = self.make_run_object(
            schema, predictor, dataset, user, date_range, group_params)
        predictor = run.load_predictor()
        pipeline = run.make_pipeline(self.transformers)
        X = pipeline.fit_transform(X)
        output = predictor.predict(X)
        return self.serialize_result(output)
