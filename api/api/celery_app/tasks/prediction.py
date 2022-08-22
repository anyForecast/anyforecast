from abc import ABCMeta

import pandas as pd

from .base_task import BaseTask
from ..dataloaders import MlFlowLoader
from ..serialize import PandasSerializer


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(
            self,
            serializer=PandasSerializer(serializer_model='to_records'),
            task_name=None,
            bind=False
    ):
        super().__init__(serializer, task_name, bind)
        self.mlflow_loader = MlFlowLoader()

    def predict(self, X, predictor):
        mlflow_predictor = self.load_mlflow_predictor(predictor)
        return mlflow_predictor.predict(X)

    def pivot_timeseries_data(self, data, group_ids, target, timestamp):
        return pd.pivot(data, columns=group_ids, values=target,
                        index=timestamp).reset_index()

    def load_mlflow_predictor(self, predictor, stage='production'):
        model_name = predictor['task_id']
        return self.mlflow_loader.load_predictor(model_name, stage)


class FromDataset(BasePredictionTask):

    def __init__(self):
        super().__init__(task_name='PredictionFromDataset')

    def run(self, predictor, dataset, user, date_range, prediction_params,
            pivot):
        partition_filter = self._make_partition_filter(prediction_params)
        pandas_loader = self.get_dataframe_loader('pandas', dataset, user)
        return pandas_loader, partition_filter

    def _create_group_transformer(self):
        pass

    def _get_group_ids(self, prediction_params):
        group_ids = []
        for param in prediction_params:
            group_id = param['group_id']
            group_ids.append(group_id)
        return group_ids

    def _make_partition_filter(self, prediction_params):
        group_ids = self._get_group_ids(prediction_params)

        def filter_function(partition):
            return partition in group_ids

        return filter_function
