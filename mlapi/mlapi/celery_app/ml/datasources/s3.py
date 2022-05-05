"""
S3 datasources.
"""

import os

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


class ParquetLoader:
    """Loader for parquet datasets stored in minio buckets.

    On each bucket, parquet datasets are stored inside the path
    `PARQUET_DATA_PATH`. Each object inside such path represents a base
    directory for a parquet partition.

    Parameters
    ----------
    bucket_name : str
        Name of bucket where parquet data is stored.

    client_args : mlapi.services.client_args.ClientArgs
        Client args provider.
    """
    PARQUET_DATA_PATH = 'data/'

    def __init__(self, bucket_name, client_args):
        self.bucket_name = bucket_name
        self.client_args = client_args

    def load(self, partition_path):
        """Loads parquet dataset given a partition path.

        To list available partition paths use method
        :meth:`get_available_partitions`.

        Parameters
        ----------
        partition_path : str
        """
        # Make a complete s3 path by adding the s3://<bucket_name> prefix.
        args = os.path.normpath(partition_path).split('/')
        partition_path = self._make_s3_path(*args)

        fs = self._get_s3_filesystem()
        parquet_dataset = pq.ParquetDataset(partition_path, filesystem=fs)
        return parquet_dataset

    def load_all(self):
        """Obtains all available parquet datasets.

        Returns
        -------
        datasets : dict, str -> pyarrow.parquet.ParquetDataset
        """
        datasets = {}
        partition_paths = self.get_available_partitions()
        for path in partition_paths:
            path_end = get_last_element_from_path(path)
            parquet_dataset = self.load(path)
            datasets[path_end] = parquet_dataset
        return datasets

    def get_available_partitions(self):
        """Obtains paths for all parquet partitions.

        Returns
        -------
        partitions_paths : list
            All partitions paths inside the bucket.
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

    def _create_minio_client(self):
        # Minio client args.
        client_args = {
            'access_key': self.client_args.access_key,
            'secret_key': self.client_args.secret_key,
            'endpoint': self.client_args.s3_endpoint,
            'secure': self.client_args.secure
        }
        return Minio(**client_args)

    def _make_s3_path(self, *args):
        """Constructs s3 path starting with "s3://sample/".

        Every arg passed is joined inserting '/' as needed.
        """
        base = "s3://{}".format(self.bucket_name)
        path = os.path.join(base, *args)
        return path

    def _get_s3_filesystem(self):
        endpoint_url = self.client_args.get_s3_endpoint()

        # s3 filesystem client args.
        fs_client_args = {
            "endpoint_url": endpoint_url,
            "aws_access_key_id": self.client_args.access_key,
            "aws_secret_access_key": self.client_args.secret_key,
            "verify": False
        }

        fs = s3fs.S3FileSystem(anon=False, use_ssl=False,
                               client_kwargs=fs_client_args)
        return fs