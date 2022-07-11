import io
import json
from abc import ABCMeta, abstractmethod

import s3fs
from minio import Minio
from pyarrow import parquet as pq

from client.services._dataset import make_dataset
from client.services.base import BaseService
from ._auth import Authenticator
from ..args import CloudArgsCreator
from ..credentials import Credentials


def make_writer(name, writer_service):
    """Factory function for writers.

    Parameters
    ----------
    name : str
        Writer name.

    writer_service : WriterService
        :class:`client.services.writer.WriterService` instance.

    Returns
    -------
    writer : BucketWriter
    """
    writers = {
        'parquet': ParquetWriter,
        'csv': CsvWriter,
        'json': JsonWriter
    }
    return writers[name](writer_service)


class BucketWriter(metaclass=ABCMeta):
    """Base abstract class for bucket writers.

    Derived classes are not meant to be constructed
    directly. Instead, instances of derived classes are constructed and
    returned from :py:func:`make_writer`.
    """

    BUCKET_NAME = 'datasets'

    def __init__(self, writer_service):
        self.writer_service = writer_service

    @abstractmethod
    def write(self, data, path):
        """Writes dataset to bucket cloud.

        Parameters
        ----------
        data : object
            Data to write.
        """
        pass

    def create_s3_filesystem(self):
        cloud_client_args = self.create_cloud_args('S3FileSystem')
        return s3fs.S3FileSystem(anon=False, use_ssl=False,
                                 client_kwargs=cloud_client_args)

    def create_root_path(self, *args, include_s3_prefix=False):
        root_path = '/'.join(args)
        if include_s3_prefix:
            return "s3://" + root_path
        return root_path

    def create_cloud_args(self, name):
        credentials = self.writer_service.get_credentials()
        endpoint = self.writer_service.get_endpoint()
        args_creator = CloudArgsCreator.make_args_creator(name, credentials,
                                                          endpoint)
        return args_creator.create_args()


class ParquetWriter(BucketWriter):
    def __init__(self, writer_service):
        super().__init__(writer_service)

    def write(self, data, path):
        fs = self.create_s3_filesystem()
        args = [self.BUCKET_NAME] + path
        root_path = self.create_root_path(*args)
        pq.write_to_dataset(data.to_pyarrow(), root_path,
                            filesystem=fs, use_dictionary=True,
                            compression="snappy", version="2.4")


class CsvWriter(BucketWriter):
    def __init__(self, writer_service):
        super().__init__(writer_service)
        raise NotImplementedError()

    def write(self, data, path):
        pass


class JsonWriter(BucketWriter):
    def __init__(self, writer_service):
        super().__init__(writer_service)

    def write(self, data, path):
        minio_client = self._create_minio_client()
        root_path = self.create_root_path(*path)
        self._put_json(minio_client, data, root_path)

    def _create_minio_client(self):
        minio_args = self.create_cloud_args('minio')
        return Minio(**minio_args)

    def _put_json(self, minio_client, data, path):
        """
        jsonify a dict and write it as object to the bucket
        """
        # Prepare data and corresponding data stream
        data = json.dumps(data).encode("utf-8")
        data_stream = io.BytesIO(data)
        data_stream.seek(0)

        # Put data as object into the bucket
        minio_client.put_object(
            bucket_name=self.BUCKET_NAME,
            object_name=path,
            data=data_stream, length=len(data),
            content_type="application/json"
        )


class WriterService(BaseService):

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def write(self, dataset_group_name, dataset_name, data, schema=None,
              format='parquet'):
        """Writes :class:`pandas.DataFrame` to s3 bucket.

        Parameters
        ----------
        dataset_group_name : str
            Directory under which the dataset will be stored.

        dataset_name : str
            Name for identifying the dataset inside ``dataset_group_name``.

        data : pandas.DataFrame
            Data to write.

        schema : dict
            Schema for ``data``.

        format : str {'parquet', 'csv'}, default='parquet'
            Writing format.
        """
        dataset = make_dataset(data, schema)
        writer = make_writer(format, self)
        ds_path = self._create_dataset_path(
            dataset_group_name, dataset_name, format)
        writer.write(dataset, ds_path)
        if schema is not None:
            schema_path = self._create_schema_path(
                dataset_group_name, dataset_name)
            json_writer = make_writer('json', self)
            json_writer.write(schema, schema_path)

    def _create_dataset_path(self, dataset_group_name, dataset_name, format):
        return [dataset_group_name, dataset_name, format]

    def _create_schema_path(self, dataset_group_name, dataset_name):
        return [dataset_group_name, dataset_name, 'schema.json']

    def get_credentials(self):
        credentials = self._authenticate()
        return Credentials(**credentials)

    def _authenticate(self):
        authenticator = Authenticator.initialize(
            loader=self._loader, endpoint_name=None, is_secure=False,
            endpoint_url=None, access_token=self._access_token)

        return authenticator.authenticate()
