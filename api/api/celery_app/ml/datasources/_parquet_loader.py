import os

import pyarrow.parquet as pq
import s3fs
from minio import Minio

from ._path_utils import (
    get_last_element_from_path,
    list_subfolders,
    add_final_slash_to_path
)


def _make_s3_path(bucket_name, *args, include_s3_prefix=True,
                  include_final_slash=True):
    """Private function for constructing a s3 path.

    Every arg passed is joined inserting '/' as needed.

    Parameters
    ----------
    bucket_name : str
        Bucket name.

    args : Extra args passed to path.
        Every arg passed is joined inserting '/' as needed.

    include_s3_prefix : bool, default=True
        Whether or not the returned path starts with "s3://".

    include_final_slash : bool, default=True
        Whether or not the returned path contains a slash as the final
        character.

    Returns
    -------
    path : str
        S3 path.
    """
    if include_s3_prefix:
        base = "s3://{}".format(bucket_name)
    else:
        base = bucket_name
    path = os.path.join(base, *args)

    if include_final_slash:
        path = add_final_slash_to_path(path)

    return path


def _make_s3_filesystem(client_args, verify=False, annon=False, use_ssl=False,
                        **kwargs):
    """Private function for constructing s3 filesystem.

    Parameters
    ----------
    client_args : `celery_app.client_args.ClientArgs`
        Instance from :class:`celery_app.client_args.ClientArgs`.

    Returns
    -------
    :class:`s3fs.S3FileSystem` instance.
    """
    endpoint_url = client_args.get_s3_endpoint()

    # s3 filesystem client args.
    fs_client_args = {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": client_args.access_key,
        "aws_secret_access_key": client_args.secret_key,
        "verify": verify
    }

    return s3fs.S3FileSystem(anon=annon, use_ssl=use_ssl,
                             client_kwargs=fs_client_args, **kwargs)


def _create_minio_client(client_args):
    """Private function for creating a minio client.

    Parameters
    ----------
    client_args : `celery_app.client_args.ClientArgs`
        Instance from :class:`celery_app.client_args.ClientArgs`.

    Returns
    -------
    minio_client : `minio.Minio`
        Instance from :class:`minio.Minio`.
    """
    minio_args = {'access_key': client_args.access_key,
                  'secret_key': client_args.secret_key,
                  'endpoint': client_args.s3_endpoint,
                  'secure': client_args.secure}
    return Minio(**minio_args)


def _load_parquet_dataset(partition_path, **kwargs):
    """Private function for creating parquet datasets.
    """
    return pq.ParquetDataset(partition_path, **kwargs)


class MultiParquetLoader:
    """Loads multiple parquet partitions into a single dictionary.
    """

    def load(self, folder, use_legacy_dataset=False):
        """Loads multiple parquet partitions into a single dictionary.

        Parameters
        ----------
        folder : str
            Path to folder containing multiple parquet partitions.

        Returns
        -------
        dict : str -> pyarrow.parquet._ParquetDatasetV2
        """
        d = {}
        subfolders = list_subfolders(folder)

        # Each element from `subfolders` is a partition for a parquet dataset.
        for sub in subfolders:
            name = get_last_element_from_path(sub)
            d[name] = _load_parquet_dataset(
                sub, use_legacy_dataset=use_legacy_dataset)
        return d

    def load_from_s3(self, client_args, bucket_name, prefix):
        """Loads multiple parquet partitions from a s3 bucket into a single
        dictionary.

        Parameters
        ----------
        client_args : celery_app.client_args.ClientArgs
            Used to create minio client.

        bucket_name : str
            Name of the bucket.

        prefix : str
            Parquet partition objects start with this prefix.
        """
        d = {}

        # Get objects inside s3 location.
        minio_client = _create_minio_client(client_args)
        objects = minio_client.list_objects(bucket_name, prefix=prefix)

        # Make s3 filesystem.
        fs = _make_s3_filesystem(client_args)

        for obj in objects:
            object_name = obj.object_name
            partition_args = os.path.normpath(object_name).split('/')
            partition_path = _make_s3_path(bucket_name, *partition_args,
                                           include_s3_prefix=False)
            name = get_last_element_from_path(partition_path)
            d[name] = _load_parquet_dataset(
                partition_path, use_legacy_dataset=False, filesystem=fs)

        return d

    def make_prefix(self, args):
        prefix = '/'.join(args)

        # Important: add final slash!
        prefix = add_final_slash_to_path(prefix)

        return prefix
