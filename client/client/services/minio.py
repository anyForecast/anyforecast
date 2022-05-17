import s3fs
from minio import Minio
from pyarrow import parquet as pq

from client.services._dataset import make_dataset
from client.services.base import BaseService
from ._auth import Authenticator
from ..credentials import Credentials


class MinioClient(BaseService):
    DATASETS_BUCKET = 'datasets'

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def create_dataset(self, dataset_group_name, dataset_name, data, schema,
                       dataset_type):
        """Writes dataset to s3 bucket.

        Parameters
        ----------
        dataset_group_name : str
            "Folder" under which the dataset will be stored.

        dataset_name : str
            Dataset name. Name for identifying the dataset inside the project.

        data : pd.DataFrame
            Data to upload to s3.

        schema : dict
            Schema for ``data``.

        dataset_type : str, {'target'}
            Dataset type.
        """
        minio_client = self._create_minio_client()
        if not minio_client.bucket_exists(bucket_name=self.DATASETS_BUCKET):
            minio_client.make_bucket(bucket_name=self.DATASETS_BUCKET)

        dataset = make_dataset(data, schema, dataset_type)
        return self._write_dataset_to_s3(dataset, dataset_group_name,
                                         dataset_name)

    def _create_minio_client(self):
        credentials = self._get_credentials()
        args_creator = MinioArgsCreator(self.endpoint, credentials)
        client_args = args_creator.create_args()
        return Minio(**client_args)

    def _get_credentials(self):
        credentials = self._authenticate()
        return Credentials(**credentials.json())

    def _authenticate(self):
        authenticator = Authenticator.initialize(
            loader=self.loader, service_name='authenticator',
            endpoint_name=None, is_secure=False, endpoint_url=None,
            access_token=self.access_token)

        return authenticator.authenticate()

    def _get_s3_filesystem(self):
        credentials = self._get_credentials()
        args_creator = MinioArgsCreator(self.endpoint, credentials)
        client_args = args_creator.create_args_for_s3_filesystem()
        return s3fs.S3FileSystem(
            anon=False,
            use_ssl=False,
            client_kwargs=client_args
        )

    def _get_dataset_root_path(self, *args, include_s3_prefix=True):
        args = list(args)
        args.insert(0, self.DATASETS_BUCKET)
        path = '/'.join(args)
        if include_s3_prefix:
            return "s3://" + path
        return path

    def _write_dataset_to_s3(self, dataset, dataset_group_name, dataset_name):
        fs = self._get_s3_filesystem()

        # Notice `dataset_root_path` has the form:
        # <DATASETS_BUCKET>/<dataset_group_name>/<dataset_name>/<dataset.type>
        args = [dataset_group_name, dataset_name, dataset.type]
        dataset_root_path = self._get_dataset_root_path(
            *args, include_s3_prefix=False)

        pq.write_to_dataset(dataset.to_pyarrow(), dataset_root_path,
                            filesystem=fs, use_dictionary=True,
                            compression="snappy", version="2.4")


class MinioArgsCreator:
    def __init__(self, endpoint, credentials):
        self._endpoint = endpoint
        self._credentials = credentials

    def create_args(self):
        if hasattr(self._credentials, 's3_endpoint'):
            endpoint = self._credentials.s3_endpoint
        else:
            _, endpoint = self._split_host()
        return {
            "endpoint": endpoint,
            "access_key": self._credentials.access_key,
            "secret_key": self._credentials.secret_key,
            "secure": self._is_secure()
        }

    def create_args_for_s3_filesystem(self):
        return {
            "endpoint_url": self._endpoint.host,
            "aws_access_key_id": self._credentials.access_key,
            "aws_secret_access_key": self._credentials.secret_key,
            "verify": False
        }

    def _split_host(self):
        protocol, endpoint = self._endpoint.host.split('://')
        return protocol, endpoint

    def _is_secure(self):
        protocol, _ = self._split_host()
        if protocol == 'https':
            return True
        elif protocol == 'http':
            return False
        else:
            raise ValueError(f'Invalid protocol: {protocol}')
