import os

import s3fs
from boto3 import Session
from minio import Minio
from minio.credentials import StaticProvider, EnvAWSProvider, ChainedProvider


class UserSession:
    """User session.

    Manages user credentials and configuration.

    Parameters
    ----------
    username : str
        Name of user
    """

    def __init__(
            self, username=None, email=None, full_name=None, access_key=None,
            secret_key=None, minio_endpoint=None, secure=False
    ):
        self.username = username
        self.email = email
        self.full_name = full_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.minio_endpoint = minio_endpoint
        self.secure = secure

    def get_minio_endpoint(self, add_scheme=True):
        minio_endpoint = (
            self.minio_endpoint if self.minio_endpoint is not None
            else os.environ.get('WR_S3_ENDPOINT_URL')
        )

        if minio_endpoint is None:
            raise ValueError(
                'A bucket endpoint must be configured using the '
                '`minio_endpoint` param or through the "WR_S3_ENDPOINT_URL" '
                'env variable.'
            )

        if add_scheme:
            scheme = 'https' if self.secure else 'http'
            return '%s://%s' % (scheme, minio_endpoint)
        return minio_endpoint

    def get_credentials(self):
        credentials_provider = self._make_credentials_provider()
        return credentials_provider.retrieve()

    def _make_credentials_provider(self):
        providers = [
            EnvAWSProvider()
            # Add providers in desired order of retrieval.
        ]

        # Static provider is given first priority.
        if self.access_key is not None:
            static_provider = StaticProvider(
                access_key=self.access_key, secret_key=self.secret_key)
            providers.insert(0, static_provider)

        return ChainedProvider(providers)

    def create_minio_client(self, **kwargs):
        return Minio(credentials=self._make_credentials_provider(),
                     endpoint=self.get_minio_endpoint(add_scheme=False),
                     secure=self.secure, **kwargs)

    def create_s3fs(self, **kwargs):
        credentials = self.get_credentials()
        client_kwargs = {
            "endpoint_url": self.get_minio_endpoint(),
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key
        }
        return s3fs.S3FileSystem(
            anon=False, use_ssl=False, client_kwargs=client_kwargs, **kwargs)

    def create_boto3_session(self):
        credentials = self.get_credentials()
        return Session(
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key)
