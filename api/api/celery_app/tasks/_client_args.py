def create_client_args(user):
    """Auxiliary function for creating a :class:`ClientArgs` instance.
    """
    print(user)
    client_args = {
        "s3_endpoint": user['s3_endpoint'],
        "access_key": user['access_key'],
        "secret_key": user['secret_key'],
        'secure': False
    }
    return ClientArgs(**client_args)


class ClientArgs:
    """Client args provider.
    """
    DEFAULT_S3_ENDPOINT = 'minio:9000'

    def __init__(self, access_key, secret_key, s3_endpoint=None, secure=True):
        self.access_key = access_key
        self.secret_key = secret_key
        if s3_endpoint is None:
            s3_endpoint = self.DEFAULT_S3_ENDPOINT
        self.s3_endpoint = s3_endpoint
        self.secure = secure

    def get_s3_endpoint(self, add_scheme=True):
        if self.secure:
            scheme = 'https'
        else:
            scheme = 'http'

        if add_scheme:
            return '%s://%s' % (scheme, self.s3_endpoint)
        return self.s3_endpoint

    def for_minio(self):
        return {
            'access_key': self.access_key,
            'secret_key': self.secret_key,
            'endpoint': self.s3_endpoint,
            'secure': self.secure
        }

    def for_s3_fs(self, verify=False):
        return {
            "endpoint_url": self.get_s3_endpoint(),
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "verify": verify
        }
