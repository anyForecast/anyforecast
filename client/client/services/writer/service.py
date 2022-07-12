import s3fs
from minio import Minio

from client.credentials import Credentials
from client.services._auth import Authenticator
from client.services.base import BaseService
from ._validations import validate_dataframe_and_schema
from ._wrappers import make_dataframe
from ...args import CloudArgsCreator


class WriterService(BaseService):

    BUCKET_NAME = 'datasets'

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

        schema : client.schema.Schema
            :class:`Schema` object.

        format : str {'parquet', 'csv'}, default='parquet'
            Writing format.
        """
        dataframe = make_dataframe(data)
        validate_dataframe_and_schema(dataframe, schema)
        path = self._create_df_path(dataset_group_name, dataset_name, format)
        fs = self._create_s3_filesystem()
        dataframe.write.parquet(path, fs)

        if schema is not None:
            path = self._create_schema_path(dataset_group_name, dataset_name)
            minio_client = self._create_minio_client()
            schema.write.json(path, minio_client, self.BUCKET_NAME)

    def _create_df_path(self, dataset_group_name, dataset_name, format):
        args = [self.BUCKET_NAME, dataset_group_name, dataset_name, format]
        return self._create_path(*args)

    def _create_schema_path(self, dataset_group_name, dataset_name):
        args = [dataset_group_name, dataset_name, 'schema.json']
        return self._create_path(*args)

    def get_credentials(self):
        credentials = self._authenticate()
        return Credentials(**credentials)

    def _authenticate(self):
        authenticator = Authenticator.initialize(
            loader=self._loader, endpoint_name=None, is_secure=False,
            endpoint_url=None, access_token=self._access_token)

        return authenticator.authenticate()

    def _create_s3_filesystem(self):
        cloud_client_args = self._create_cloud_args('S3FileSystem')
        return s3fs.S3FileSystem(anon=False, use_ssl=False,
                                 client_kwargs=cloud_client_args)

    def _create_path(self, *args):
        return '/'.join(args)

    def _create_cloud_args(self, name):
        credentials = self.get_credentials()
        endpoint = self.get_endpoint()
        args_creator = CloudArgsCreator.make_args_creator(name, credentials,
                                                          endpoint)
        return args_creator.create_args()

    def _create_minio_client(self):
        minio_args = self._create_cloud_args('minio')
        return Minio(**minio_args)
