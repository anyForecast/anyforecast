import io
import json
import os
from abc import ABCMeta, abstractmethod

import pyarrow.parquet as pq
import s3fs
from minio import Minio


class DataLoader:

    def __init__(self, client_args):
        self._parquet_loader = ParquetLoader(client_args)
        self._schema_loader = SchemaLoader(client_args)

    def load_parquet(self, bucket_name, dataset_group_name, dataset_name,
                     schema=None):
        return self._parquet_loader.load(bucket_name, dataset_group_name,
                                         dataset_name, schema)

    def load_schema(self, bucket_name, dataset_group_name, dataset_name):
        return self._schema_loader.load(bucket_name, dataset_group_name,
                                        dataset_name)


class Loader(metaclass=ABCMeta):
    """Base abstract class for Loaders.
    """

    def __init__(self, client_args):
        self.client_args = client_args

    @abstractmethod
    def load(self, bucket_name, dataset_group_name, dataset_name):
        pass

    def _make_path(self, bucket_name, dataset_group_name, dataset_name, *args):
        args = [dataset_group_name, dataset_name] + list(args)
        if bucket_name is not None:
            args.insert(0, bucket_name)
        return self._path_join(*args)

    def _path_join(self, *args):
        return os.path.join(*args)

    def _make_filesysyem(self, **kwargs):
        fs_client_args = self.client_args.for_s3_fs()
        return s3fs.S3FileSystem(anon=False, use_ssl=False,
                                 client_kwargs=fs_client_args,
                                 **kwargs)

    def _create_minio_client(self):
        """Private function for creating a minio client.

        Returns
        -------
        minio_client : `minio.Minio`
            Instance from :class:`minio.Minio`.
        """
        minio_client_args = self.client_args.for_minio()
        return Minio(**minio_client_args)


class ParquetLoader(Loader):
    def __init__(self, client_args):
        super().__init__(client_args)

    def load(self, bucket_name, dataset_group_name, dataset_name, schema=None):
        path = self._make_path(bucket_name, dataset_group_name, dataset_name,
                               'parquet')
        fs = self._make_filesysyem()
        return pq.ParquetDataset(path, fs, use_legacy_dataset=False,
                                 schema=schema)


class SchemaLoader(Loader):
    def __init__(self, client_args):
        super().__init__(client_args)

    def load(self, bucket_name, dataset_group_name, dataset_name):
        path = self._make_path(None, dataset_group_name, dataset_name,
                               'schema.json')
        return self.get_json(bucket_name, path)

    def get_json(self, bucket_name, object_name):
        """Get stored json object from the bucket.
        """
        minio_client = self._create_minio_client()
        data = minio_client.get_object(bucket_name, object_name)
        return json.load(io.BytesIO(data.data))
