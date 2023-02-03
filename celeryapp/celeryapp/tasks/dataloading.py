from ._base_task import BaseTask
from ..dataloaders import FeaturesSchema
from ..dataloaders import create_dataloader


class LoadPandas(BaseTask):
    """

    Notes
    -----
    Since ´ignore_result=True´, even though the task is completed by the worker,
    the state of the task will be always PENDING.
    """
    def __init__(self):
        super().__init__(ignore_result=True)

    def _make_partition_filter(self, partitions):
        if partitions is None:
            return None

        def filter_function(partition):
            return partition in partitions

        return filter_function

    def run(self, dataset, user, partitions=None, return_schema=True):
        pandas = self._load_pandas(dataset, user, partitions)
        result = {'DataFrame': pandas}

        if return_schema:
            features_schema = self._load_schema(dataset, user)
            result['FeaturesSchema'] = FeaturesSchema(features_schema)

        return result

    def _load_pandas(self, dataset, user, partitions):
        parquet_loader = create_dataloader(user, dataloader_name='parquet')
        parquet_path = self._make_parquet_path(dataset)
        partition_filter = self._make_partition_filter(partitions)
        return parquet_loader.load_pandas(
            path=parquet_path,
            partition_filter=partition_filter
        )

    def _load_schema(self, dataset, user):
        json_loader = create_dataloader(user, dataloader_name='json')
        schema_path = self._make_schema_path(dataset)
        return json_loader.load(
            bucket_name=dataset['bucket_name'],
            object_name=schema_path
        )

    def _make_parquet_path(self, dataset, s3_prefix=True):
        path = "{bucket_name}/{dataset_group_name}/{dataset_name}/parquet"
        path = path.format(**dataset)

        if s3_prefix:
            return "s3://" + path
        return path

    def _make_schema_path(self, dataset):
        path = "{dataset_group_name}/{dataset_name}/schema.json"
        return path.format(**dataset)
