from abc import ABCMeta

import pandas as pd
from sklearn.pipeline import make_pipeline
from skorch_forecasting.utils.datetime import set_date_on_freq

from ._base_task import BaseTask
from ..dataloaders import MlFlowLoader
from ..ml.transformers import GroupWiseDatetimeLocator, GroupWiseWhatIf
from ..serialize.pandas import PandasNativeSerializer


class TransformersFactory:

    def __init__(self, run):
        self.run = run

    def make_transformer(self, name, schema):
        transformers = {
            'datetime_locator': self._make_datetime_locator,
            'what_if': self._make_what_if
        }
        return transformers[name](schema)

    def _make_what_if(self, schema):
        """Factory for :class:`GroupWiseWhatIf` instances.

        Returns
        -------
        what_if : GroupWiseWhatIf
            Transformer GroupWiseWhatIf
        """
        what_if_data = self.run.get_what_if_data(schema)
        group_ids = schema.group_ids.names
        timestamp = schema.timestamp.names[0]
        _, forecast_date_range = self.run.split_date_range()
        return GroupWiseWhatIf(group_ids, forecast_date_range, timestamp,
                               what_if_data)

    def _make_datetime_locator(self, schema):
        encoder_date_range, forecast_date_range = self.run.split_date_range()
        full_date_range = encoder_date_range.union(forecast_date_range)
        group_ids = schema.group_ids.names
        timestamp = schema.timestamp.names[0]
        return GroupWiseDatetimeLocator(
            group_ids=group_ids,
            timestamp_col=timestamp,
            date_range=full_date_range
        )


class PredictionRun:

    def __init__(self, task, predictor, dataset, user, date_range,
                 group_params):
        self.task = task
        self.predictor = predictor
        self.dataset = dataset
        self.user = user
        self.date_range = date_range
        self.group_params = group_params
        self.mlflow_loader = MlFlowLoader()
        self.model_params = self._get_model_params()

    def make_pipeline(self, schema, transformers_names):
        factory = TransformersFactory(self)

        steps = []
        for name in transformers_names:
            transformer = factory.make_transformer(name, schema)
            steps.append(transformer)

        return make_pipeline(*steps)

    def _get_model_params(self):
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
        model_name = self.predictor['model_name']
        return self.mlflow_loader.load_predictor(model_name, stage)

    def split_date_range(self):
        """Splits date range.

        Returns
        -------
        date_range : pd.DateTimeIndex
        """
        encoder_length = int(self.model_params['max_encoder_length'])
        freq = self.model_params['freq']

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

    def load_pandas_and_schema(self):
        filter_fn = self.make_partition_filter()
        return self.task.load_pandas_and_schema(self.dataset, self.user,
                                                filter_fn)

    def get_what_if_data(self, schema):
        """Obtains what_if data from ``prediction_params``.

        Returns
        -------
        what_if_data : dict, str -> dict
        """
        group_ids = schema.group_ids.names
        what_if_data = {}
        for group_data in self.group_params:
            # Obtain group id values.
            group_id = group_data['group_id']
            group_id = [group_id[g] for g in group_ids]
            group_id = group_id[0] if len(group_id) == 1 else tuple(group_id)

            if 'what_if' in group_data:
                what_if_value = group_data['what_if']
                if what_if_value is not None:
                    what_if_data[group_id] = what_if_value

        return what_if_data

    def make_partition_filter(self):
        """Makes a partition filter function.

        This function in given to the dataloader for loading only the
        requested partitions.

        Returns
        -------
        filter_function : callable
        """
        # Collect group_ids from ``group_params``.
        group_ids = []
        for param in self.group_params:
            group_id = param['group_id']
            group_ids.append(group_id)

        if not group_ids:
            return None

        def filter_function(partition):
            return partition in group_ids

        return filter_function


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(self, serializer=PandasNativeSerializer(), task_name=None,
                 bind=False):
        super().__init__(serializer, task_name, bind)

    def make_run_object(self, predictor, dataset, user, date_range,
                        group_params):
        return PredictionRun(self, predictor, dataset, user, date_range,
                              group_params)


class GroupPrediction(BasePredictionTask):
    def __init__(self):
        super().__init__(
            serializer=PandasNativeSerializer(orient='records'),
            task_name='GroupPrediction'
        )
        self.transformers = ['datetime_locator', 'what_if']

    def run(self, predictor, dataset, user, date_range, group_params,
            return_truth):
        run = self.make_run_object(predictor, dataset, user, date_range,
                                   group_params)

        X, schema = run.load_pandas_and_schema()
        predictor = run.load_predictor()
        pipeline = run.make_pipeline(schema, self.transformers)
        X = pipeline.fit_transform(X)
        output = predictor.predict(X)
        return self.serialize_result(output)
