from ..base import BaseService


class Forecast(BaseService):
    def __init__(self, endpoint, loader, credentials, s3_endpoint):
        super().__init__(endpoint, loader, credentials, s3_endpoint)




