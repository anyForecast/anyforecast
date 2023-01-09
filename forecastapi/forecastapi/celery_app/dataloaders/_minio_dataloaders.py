import io
import json
import os
from abc import ABCMeta

import s3fs
from minio import Minio

from ._parquet_loaders import make_parquet_loader
from ..client_args import ClientArgsCreator


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


class PathMaker:
    def __init__(self):
        pass

    def join_base_dir(self, base_dir, path, trailing_slash=False):
        return self.make_path(base_dir, path, trailing_slash=trailing_slash)

    def make_path(self, *args, trailing_slash=False):
        return _make_path(*args, trailing_slash=trailing_slash)

    def make_json_path(self, base_dir, path):
        if not path.endswith('.json'):
            path += '.json'
        return self.join_base_dir(base_dir, path)

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


class MinioDataloadersFactory:
    """Factory for Minio dataloaders.

    The actual "dataloaders" are accessible through :meth:´get_loader´.
    Additionally, each of these loaders inherits from the base abstract class
    :class:`MinioDataloader`.

    Parameters
    ----------
    user : User
    dataset : Dataset
    """

    def __init__(self, user, dataset):
        self.user = user
        self.dataset = dataset

    def get_dataloader(self, name):
        return self.dataloaders[name](**self._make_kwargs())

    def _make_kwargs(self):
        return {
            'client_args': ClientArgsCreator(self.user, secure=False),
            'bucket_name': self.dataset['bucket_name'],
            'dataset_group_name': self.dataset['dataset_group_name'],
            'dataset_name': self.dataset['dataset_name']
        }

    @property
    def dataloaders(self):
        return {
            'parquet': ParquetLoader,
            'json': JsonLoader,
            'objects': ObjectsLoader
        }


class MinioDataloader(metaclass=ABCMeta):
    """Base abstract class for Minio dataloaders.

    Different file extensions (parquet, json, etc) have their own derived class
    containing the logic to load them from Minio.

    Parameters
    ----------
    """

    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        self.client_args = client_args
        self.bucket_name = bucket_name
        self.dataset_group_name = dataset_group_name
        self.dataset_name = dataset_name
        self._path_maker = PathMaker()

    def create_minio_client(self):
        """Private function for creating a minio client.

        Returns
        -------
        minio_client : `minio.Minio`
            Instance from :class:`minio.Minio`.
        """
        minio_client_args = self.client_args.for_minio()
        return Minio(**minio_client_args)

    def get_base_dir(self, bucket_name=True, s3_prefix=False):
        path_args = [
            self.dataset_group_name,
            self.dataset_name
        ]

        if bucket_name:
            path_args.insert(0, self.bucket_name)

        path = self._path_maker.make_path(*path_args, trailing_slash=True)
        if s3_prefix:
            path = 's3://' + path
        return path

    def make_s3_filesystem(self, **kwargs):
        fs_client_args = self.client_args.for_s3_fs()
        return s3fs.S3FileSystem(
            anon=False, use_ssl=False, client_kwargs=fs_client_args,
            **kwargs)


class ParquetLoader(MinioDataloader):
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

    def _make_parquet_loader(self, name):
        base_dir = self.get_base_dir(s3_prefix=True)
        base_dir = self._path_maker.join_base_dir(
            base_dir, 'parquet', trailing_slash=True)
        fs = self.make_s3_filesystem()

        kwargs = {
            'base_dir': base_dir,
            's3_filesystem': fs
        }
        return make_parquet_loader(name, **kwargs)

    @property
    def to_pandas(self):
        return self._make_parquet_loader('pandas')

    @property
    def to_spark(self):
        return self._make_parquet_loader('spark')


class JsonLoader(MinioDataloader):
    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        super().__init__(client_args, bucket_name, dataset_group_name,
                         dataset_name)

    def load(self, path):
        base_dir = self.get_base_dir(bucket_name=False)
        json_path = self._path_maker.make_json_path(base_dir, path)
        return self._get_json(json_path)

    def _get_json(self, object_name):
        """Get stored json object from the bucket.
        """
        minio_client = self.create_minio_client()
        json_object = minio_client.get_object(self.bucket_name, object_name)
        return json.load(io.BytesIO(json_object.data))


class ObjectsLoader(MinioDataloader):
    def __init__(self, client_args, bucket_name, dataset_group_name,
                 dataset_name):
        super().__init__(client_args, bucket_name, dataset_group_name,
                         dataset_name)

    def load(self, extra_prefix=None):
        prefix = self.get_base_dir(bucket_name=False)

        if extra_prefix is not None:
            prefix = self._path_maker.make_path(
                prefix, extra_prefix, trailing_slash=True)

        minio_client = self.create_minio_client()
        return minio_client.list_objects(self.bucket_name, prefix=prefix)
