import os

import pyarrow.parquet as pq

from ..utils.s3 import create_minio_client, make_s3_path, make_s3_filesystem
from ..utils.path import (
    get_last_element_from_path,
    list_subfolders,
    add_final_slash_to_path
)


class ParquetLoader:
    """Loader for parquet datasets.
    """

    def load(self, partition_path, **kwargs):
        """Loads parquet dataset given a partition path.

        Parameters
        ----------
        partition_path : str
        """
        parquet_dataset = pq.ParquetDataset(
            partition_path, use_legacy_dataset=False, **kwargs)
        return parquet_dataset

    def load_many(self, folder):
        many = {}
        subfolders = list_subfolders(folder)

        # Each element from `subfolders` is a partition for a parquet dataset.
        for sub in subfolders:
            name = get_last_element_from_path(sub)
            many[name] = self.load(sub)
        return many


class S3ParquetLoader:
    def __init__(self, client_args):
        self.client_args = client_args
        self._loader = ParquetLoader()

    def load(self, partition_path, filesystem):
        return self._loader.load(partition_path, filesystem=filesystem)

    def load_many(self, bucket_name, *args):
        many = {}
        minio_client = create_minio_client(self.client_args)
        prefix = self._make_prefix(args)
        objects = minio_client.list_objects(bucket_name, prefix=prefix)
        fs = make_s3_filesystem(self.client_args)

        for obj in objects:
            object_name = obj.object_name
            partition_args = os.path.normpath(object_name).split('/')
            partition_path = make_s3_path(bucket_name, *partition_args,
                                          include_s3_prefix=False)
            name = get_last_element_from_path(partition_path)
            many[name] = self.load(partition_path, fs)

        return many

    def _make_prefix(self, args):
        prefix = '/'.join(args)

        # Important: add final slash!
        prefix = add_final_slash_to_path(prefix)

        return prefix

