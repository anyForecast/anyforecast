from typing import Optional, List, Tuple, Dict, Union, TypedDict

import pandas as pd

from . import base, schemas
from .. import dataloaders


class Dataset(TypedDict):
    bucket_name: str
    dataset_group_name: str
    dataset_name: str


class User(TypedDict):
    username: Optional[str]
    email: Optional[str]
    full_name: Optional[str]
    access_key: Optional[str]
    secret_key: Optional[str]
    minio_endpoint: Optional[str]


class LoadPandas(base.BaseTask):
    """Loads pandas DataFrame.

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

    def run(
            self,
            dataset: Dataset,
            user: User,
            partitions: Optional[List[Dict]] = None,
            return_schema: bool = True
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, schemas.FeaturesSchema]]:
        """Loads pandas DataFrame.

        Parameters
        ----------
        dataset
        user
        partitions
        return_schema

        Returns
        -------
        """

        dataloader_creator = dataloaders.DataloaderCreator(user)
        pandas = self._load_pandas(dataloader_creator, dataset, partitions)

        if return_schema:
            schema = self._load_schema(dataloader_creator, dataset)
            schema = schemas.FeaturesSchemaCreator(schema).create_schema()
            return pandas, schema
        return pandas

    def _load_pandas(self, dataloader_creator, dataset, partitions):
        parquet_loader = dataloader_creator.create_dataloader('parquet')
        parquet_path = self._make_parquet_path(dataset)
        partition_filter = self._make_partition_filter(partitions)
        return parquet_loader.load_pandas(
            path=parquet_path, partition_filter=partition_filter)

    def _load_schema(self, dataloader_creator, dataset):
        json_loader = dataloader_creator.create_dataloader('json')
        schema_path = self._make_schema_path(dataset)
        return json_loader.load(
            bucket_name=dataset['bucket_name'], object_name=schema_path)

    def _make_parquet_path(self, dataset, s3_prefix=True):
        path = "{bucket_name}/{dataset_group_name}/{dataset_name}/parquet"
        path = path.format(**dataset)

        if s3_prefix:
            return "s3://" + path
        return path

    def _make_schema_path(self, dataset):
        path = "{dataset_group_name}/{dataset_name}/schema.json"
        return path.format(**dataset)
