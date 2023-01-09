from minio.credentials import StaticProvider, EnvAWSProvider, ChainedProvider


class ClientArgsCreator:
    """Client args creator.
    """
    DEFAULT_MINIO_ENDPOINT = 'minio:9000'

    def __init__(self, user, secure=True):
        self.user = user
        self.secure = secure

    def get_minio_endpoint(self, add_scheme=True):
        minio_endpoint = self.user.get('minio_endpoint')
        if minio_endpoint is None:
            minio_endpoint = self.DEFAULT_MINIO_ENDPOINT

        if self.secure:
            scheme = 'https'
        else:
            scheme = 'http'

        if add_scheme:
            return '%s://%s' % (scheme, minio_endpoint)
        return minio_endpoint

    def make_credentials_provider(self):
        providers = [
            EnvAWSProvider()
            # Add providers in desired order of retrieval.
        ]

        # Static provider is given first priority.
        if self.user['access_key']:
            static_provider = StaticProvider(
                access_key=self.user['access_key'],
                secret_key=self.user['secret_key']
            )
            providers.insert(0, static_provider)

        return ChainedProvider(providers)

    def for_minio(self):
        provider = self.make_credentials_provider()
        return {
            'credentials': provider,
            'endpoint': self.get_minio_endpoint(False),
            'secure': self.secure
        }

    def for_s3_fs(self, verify=False):
        credentials = self.make_credentials_provider().retrieve()
        return {
            "endpoint_url": self.get_minio_endpoint(),
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
            "verify": verify
        }
