import io
import json
import os
from abc import ABCMeta

import s3fs
from minio import Minio

from ._parquet_loaders import make_parquet_loader
from ._schema_resolver import SchemaResolver


def _make_path(*args, trailing_slash=False):
    path = os.path.join(*args)
    if trailing_slash:
        path += '/'
    return path


def _recursive_concat(path, trailing_slash=True, **kwargs):
    if not kwargs:
        return path

    if len(kwargs) == 1:
        items = list(kwargs.items())[0]
        extra_chars = '='.join(items)
        path = os.path.join(path, extra_chars)
        if trailing_slash:
            path += '/'

    else:
        for k, v in kwargs.items():
            path = _recursive_concat(path, **{k: v})

    return path


class S3PathMaker:
    def __init__(self):
        pass

    def make_path(self, base_dir, path):
        return _make_path(base_dir, path)

    def make_json_path(self, base_dir, path):
        if not path.endswith('.json'):
            path += '.json'
        return self.make_path(base_dir, path)

    def make_parquet_path(self, base_dir, s3_filesystem, partitions=None):
        if partitions:
            paths = []
            for partition in partitions:
                path = _recursive_concat(base_dir, **partition)
                parquet_path = self._get_parquet_path(s3_filesystem, path)
                paths.extend(parquet_path)
            return paths
        return base_dir

    def _get_parquet_path(self, s3_filesystem, partition_path):
        """For partitioned data, the path to retrieve is the one inside
        the partition folder. That is

        ├── base_dir
            ├── partition_folder_0 (i.e., year=2021)
                ├── file_to_retrieve_0.parquet
            ├── partition_folder_1 (i.e., year=2022)
                ├── file_to_retrieve_1.parquet

            and so on...
        """
        return s3_filesystem.glob(partition_path)


class S3LoadersFactory:
    """Factory for S3 loaders.

    The actual "loaders" are accessible through :meth:´get_loader´.
    Additionally, each of this loaders inherits from the base abstract class
    :class:`S3BaseLoader`.

    Parameters
    ----------
    client_args : ClientArgs
        Instance of :class:`ClientArgs` object.

    bucket_name : str
        Name of bucket

    dataset_group_name : str
        Dataset group name

    dataset_name : str
        Dataset name
    """

    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):

        self.client_args = client_args
        self.bucket_name = bucket_name
        self.dataset_group_name = dataset_group_name
        self.dataset_name = dataset_name


    def get_loader(self, name):
        s3_loaders = {
            'parquet': ParquetLoader,
            'json': JsonLoader,
            'schema': SchemaLoader
        }
        kwargs = {
            'client_args': self.client_args,
            'bucket_name': self.bucket_name,
            'dataset_group_name': self.dataset_group_name,
            'dataset_name': self.dataset_name
        }

        return s3_loaders[name](**kwargs)



class S3BaseLoader(metaclass=ABCMeta):
    """Base abstract class for S3Loaders.

    Different file extensions (parquet, json, etc) have their own derived class
    containing the logic to load them from S3.

    Parameters
    ----------
    client_args : ClientArgs
        Instance of :class:`ClientArgs` object.
    """

    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        self.client_args = client_args
        self.bucket_name = bucket_name
        self.dataset_group_name = dataset_group_name
        self.dataset_name = dataset_name
        self.path_maker = S3PathMaker()

    def create_minio_client(self):
        """Private function for creating a minio client.

        Returns
        -------
        minio_client : `minio.Minio`
            Instance from :class:`minio.Minio`.
        """
        minio_client_args = self.client_args.for_minio()
        return Minio(**minio_client_args)

    def get_base_dir(self, *args, s3_prefix=False):
        path_args = [
            self.bucket_name,
            self.dataset_group_name,
            self.dataset_name
        ]
        path_args += list(args)
        path = _make_path(*path_args)
        if s3_prefix:
            path = 's3://' + path
        return path

    def make_s3_filesystem(self, **kwargs):
        fs_client_args = self.client_args.for_s3_fs()
        return s3fs.S3FileSystem(
            anon=False, use_ssl=False, client_kwargs=fs_client_args,
            **kwargs)


class ParquetLoader(S3BaseLoader):
    """Interface for parquet loaders.

    :class:`ParquetLoader` is only an interface layer for the various
    destination formats the parquet datasets can have (see Properties section).

    Properties
    ----------
    to_pandas : ParquetToPandas
        Parquet -> Pandas loader

    to_spark : ParquetToSpark
        Parquet -> Spark loader
    """

    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        super().__init__(client_args, bucket_name, dataset_group_name,
                         dataset_name)

        self._kwargs = {
            'base_dir': self.get_base_dir('parquet', s3_prefix=True),
            's3_filesystem': self.make_s3_filesystem()
        }


    def _make_parquet_loader(self, name):
        kwargs = {
            'base_dir': self.get_base_dir('parquet', s3_prefix=True),
            's3_filesystem': self.make_s3_filesystem()
        }
        return make_parquet_loader(name, **kwargs)

    @property
    def to_pandas(self):
        return self._make_parquet_loader('pandas')

    @property
    def to_spark(self):
        return self._make_parquet_loader('spark')





class JsonLoader(S3BaseLoader):
    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        super().__init__(client_args, bucket_name, dataset_group_name,
                         dataset_name)

    def load(self, path):
        base_dir = self.get_base_dir()
        json_path = self.path_maker.make_json_path(base_dir, path)
        json_path = json_path.replace(self.bucket_name, '')
        return self._get_json(json_path)

    def _get_json(self, object_name):
        """Get stored json object from the bucket.
        """
        minio_client = self.create_minio_client()
        data = minio_client.get_object(self.bucket_name, object_name)
        return json.load(io.BytesIO(data.data))


class SchemaLoader(JsonLoader):
    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        super().__init__(client_args, bucket_name, dataset_group_name,
                         dataset_name)

    def load(self, path='schema.json'):
        schema_data = super().load(path)
        return SchemaResolver(schema_data)



