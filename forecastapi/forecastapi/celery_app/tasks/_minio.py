import pandas as pd

from ._base_task import BaseTask
from ..dataloaders import S3LoadersFactory


class GetParquetPartitions(BaseTask):

    def __init__(self, serializer=None):
        super().__init__(serializer)

    def run(self, user, dataset):
        loader = S3LoadersFactory(user, dataset).get_loader('objects')

        objects = loader.load('parquet')
        partition_names = []
        for obj in objects:
            obj_name = obj.object_name
            if obj.is_dir:
                obj_name = obj_name[:-1]

            obj_name = obj_name.split('/')[-1]
            obj_name = obj_name.split('=')[-1]
            partition_names.append(obj_name)

        return partition_names


class GetLastKnownDate(BaseTask):
    FORMAT = '%Y-%m-%d'

    def __init__(self, serializer=None, task_name=None, bind=False):
        super().__init__(serializer, task_name, bind)

    def run(self, data):
        X, schema = data['X'], data['schema']
        timestamp = schema.get_names_for('timestamp')[0]

        time_human = X[timestamp].max().strftime('%Y-%m-%d')
        time_s = (pd.to_datetime([time_human]).astype(int) / 10 ** 9).item()
        time_ms = (pd.to_datetime([time_human]).astype(int) / 10 ** 6).item()
        return {
            "timeHuman": time_human,
            "timeMs": time_ms,
            "timeS": time_s,
            'text': 'Last known date'
        }
