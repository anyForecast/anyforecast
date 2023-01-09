from ._base_task import BaseTask
from ..dataloaders import SchemaResolver
from ..dataloaders import make_dataframe_loader


class LoadDatasetTask(BaseTask):
    def __init__(self):
        super().__init__()

    def _make_partition_filter(self, partitions):
        if partitions is None:
            return None

        def filter_function(partition):
            return partition in partitions

        return filter_function

    def run(self, dataset, user, partitions, format, return_schema):

        loader = make_dataframe_loader(format, dataset, user)
        partition_filter = self._make_partition_filter(partitions)
        df = loader.load(partition_filter=partition_filter)
        result = {'X': df}

        if return_schema:
            schema = loader.load_schema()
            result['schema'] = SchemaResolver(schema)

        return result
