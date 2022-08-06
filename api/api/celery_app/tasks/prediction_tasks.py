from abc import ABCMeta

import mlflow
import pandas as pd

from .base_task import BaseTask
from ..serialize import PandasSerializer


class BasePredictionTask(BaseTask, metaclass=ABCMeta):
    def __init__(
            self,
            serializer=PandasSerializer(serializer_model='to_records'),
            task_name=None,
            bind=False
    ):
        super().__init__(serializer, task_name, bind)

    def predict(self, X, predictor):
        mlflow_predictor = self.load_mlflow_predictor(predictor)
        return mlflow_predictor.predict(X)

    def pivot_timeseries_data(self, data, group_ids, target, timestamp):
        return pd.pivot(data, columns=group_ids, values=target,
                        index=timestamp).reset_index()

    def load_mlflow_predictor(self, predictor):
        model_id = predictor['task_id']
        url = f"models:/{model_id}/production"
        return mlflow.pyfunc.load_model(url)


class FromDataset(BasePredictionTask):

    def __init__(self):
        super().__init__(task_name='PredictionFromDataset')

    def run(self, predictor, dataset, user, group_ids, pivot):
        partition_filter = self._make_partition_filter(group_ids)
        pandas_loader = self.get_dataframe_loader('pandas', dataset, user)
        X = pandas_loader.load(partition_filter=partition_filter)
        output = self.predict(X, predictor)
        if pivot:
            schema = pandas_loader.load_schema()
            keys = ('group_ids', 'target', 'timestamp')
            pivot_kwargs = schema.get_names_for(keys)
            pivot_kwargs['target'] = pivot_kwargs['target'][0]
            output = self.pivot_timeseries_data(X, **pivot_kwargs)
            print(output)

        return self.serialize_result({'data': output})

    def _make_partition_filter(self, group_ids):
        def filter_function(partition):
            return partition in group_ids

        return filter_function
