"""
Module containing classes for retrieving, exploring and merging parquet
datasets. In general, it's purpose is to bridge our workflow with the parquet
format.
"""

import os
from functools import reduce

import pandas as pd
import pyarrow.parquet as pq
import s3fs
from minio import Minio


def get_last_element_from_path(path):
    """Obtains the last element from `path`.

    Reference
    ---------
    https://stackoverflow.com/questions/3925096/how-to-get-only-the-last-part-of-a-path-in-python
    """
    return os.path.basename(os.path.normpath(path))


class ParquetGetter:
    """Getter for parquet datasets stored in minio buckets.

    On each bucket, parquet datasets are stored inside the path
    `PARQUET_DATA_PATH`. Each object inside such path represents a base
    directory for a parquet partition.
    """
    PARQUET_DATA_PATH = 'data/'

    def __init__(self, bucket_name, client_args):
        self.bucket_name = bucket_name

    def get_parquet_dataset(self, partition_path):

        # Make a complete s3 path by adding the s3://<bucket_name> prefix.
        args = os.path.normpath(partition_path).split('/')
        partition_path = self._make_s3_path(*args)

        fs = self._get_s3_filesystem()
        parquet_dataset = pq.ParquetDataset(partition_path, filesystem=fs)
        return parquet_dataset

    def resolve_datasets(self, parquet_mask=True):
        resolved = {}
        partition_paths = self.get_parquet_partition_paths()
        for path in partition_paths:
            path_end = get_last_element_from_path(path)
            parquet_dataset = self.get_parquet_dataset(path)
            if parquet_mask:
                parquet_dataset = ParquetMask(parquet_dataset)
            resolved[path_end] = parquet_dataset
        return resolved

    def get_parquet_partition_paths(self):
        """Obtains paths for all parquet partitions.

        Returns
        -------
        list
        """
        client = self._create_minio_client()

        # List objects inside `PARQUET_DATA_PATH`.
        objects = client.list_objects(
            self.bucket_name, prefix=self.PARQUET_DATA_PATH)

        partition_paths = []
        for obj in objects:
            # `obj_name` contains the entire path, that is,
            # ROOT_DATA_PATH/<name>.
            obj_name = obj.object_name
            partition_paths.append(obj_name)

        return partition_paths

    def _make_endpoint(self, include_protocol=True):
        if self.is_secure:
            scheme = 'https'
        else:
            scheme = 'http'

        if include_protocol:
            return '%s://%s' % (scheme, self.MINIO_ENDPOINT)
        return self.MINIO_ENDPOINT

    def _create_minio_client(self):
        endpoint_url = self._make_endpoint(include_protocol=False)

        # Minio client args.
        client_args = {
            "endpoint": endpoint_url,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            'secure': self.is_secure
        }

        return Minio(**client_args)

    def _make_s3_path(self, *args):
        path = (
                "s3://"
                + self.bucket_name
                + '/'
                + '/'.join(args)
        )
        return path

    def _get_s3_filesystem(self):
        endpoint_url = self._make_endpoint()

        # s3 filesystem client args.
        fs_client_args = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "verify": False
        }

        fs = s3fs.S3FileSystem(anon=False, use_ssl=False,
                               client_kwargs=fs_client_args)
        return fs


class ParquetMask:
    """Interface for parquet datasets.

    Parameters
    ----------
    parquet_ds : pyarrow.parquet.ParquetDataset
    """

    def __init__(self, parquet_ds):
        self.parquet_ds = parquet_ds

    def to_pandas(self):
        return self.parquet_ds.read_pandas().to_pandas()

    def get_arrow_schema(self):
        return self.parquet_ds.schema.to_arrow_schema()

    def get_names(self):
        arrow_schema = self.get_arrow_schema()
        return arrow_schema.names

    def merge(self, parquet_ds, on, **kwargs):
        left_df = self.to_pandas()
        right_df = parquet_ds.to_pandas()
        merged_df = pd.merge(
            left=left_df,
            right=right_df,
            on=on,
            **kwargs
        )
        return merged_df


class ParquetMerger:
    """Base class for parquet mergers.

    Parameters
    ----------
    **kwargs : pyarrow.parquet.ParquetDataset
    """

    def __init__(self, **kwargs):
        self._validate_kwargs(**kwargs)
        self.kwargs = kwargs
        vars(self).update(self.kwargs)

    def get_dataset_by_name(self, name):
        if name not in self.kwargs:
            raise ValueError('Unknown dataset "{}"'.format(name))
        return self.kwargs[name]

    def _get_datasets(self, include_nones=False):
        datasets = self.kwargs.values()
        if include_nones:
            return datasets
        return [x for x in datasets if x is not None]

    def resolve_names(self):
        names = {
            k: v.get_names()
            if v is not None
            else [] for k, v in self.kwargs.items()
        }
        return names

    def merge(self, on, **kwargs):
        datasets = self._get_datasets()

        if len(datasets) == 1:
            return datasets[0].to_pandas()

        merged_df = reduce(
            lambda left, right: left.merge(right, on, **kwargs),
            datasets)
        return merged_df

    def _validate_kwargs(self, **kwargs):
        for key, dataset in kwargs.items():
            if dataset is not None:
                if not isinstance(dataset, ParquetMask):
                    raise TypeError(
                        'All parameters must be of type Dataset. '
                        'Instead, kwarg {} received type {}'.format(key, type(
                            dataset).__name__))
