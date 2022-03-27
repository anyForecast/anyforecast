import s3fs
from minio import Minio
from pyarrow import parquet as pq

from .dataset import make_dataset


class MinioClient:
    def __init__(self, endpoint, loader, credentials):
        self.loader = loader
        self._args_creator = MinioArgsCreator(endpoint, credentials)
        self._minio_client = self._create_minio_client()

    def _create_minio_client(self):
        client_args = self._args_creator.create_args()
        return Minio(**client_args)

    def create_dataset(self, df, schema, dataset_type, name):
        if not self._minio_client.bucket_exists(bucket_name=name):
            self._minio_client.make_bucket(bucket_name=name)

        dataset = make_dataset(df, schema, dataset_type)
        self._write_dataset_to_s3(dataset, name)

    def _get_s3_filesystem(self):
        client_args = self._args_creator.create_args_for_s3_filesystem()
        return s3fs.S3FileSystem(
            anon=False,
            use_ssl=False,
            client_kwargs=client_args
        )

    def _get_s3_root_path(self, bucket_name, *args, include_s3_prefix=True):
        path = bucket_name + '/' + '/'.join(args)
        if include_s3_prefix:
            return "s3://" + path
        return path

    def _write_dataset_to_s3(self, dataset, name):
        fs = self._get_s3_filesystem()
        root_path = self._get_s3_root_path(
            name, dataset.type, include_s3_prefix=False)
        pq.write_to_dataset(dataset.to_pyarrow(), root_path, filesystem=fs,
                            use_dictionary=True, compression="snappy",
                            version="2.4")


class MinioArgsCreator:
    def __init__(self, endpoint, credentials):
        self._endpoint = endpoint
        self._credentials = credentials

    def create_args(self):
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
