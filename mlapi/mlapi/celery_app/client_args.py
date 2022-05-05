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

