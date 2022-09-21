from ...args import ClientArgsCreator
from ...endpoint import ClientEndpointBridge, EndpointResolver
from ...services.base import BaseService


class Authenticator(BaseService):
    """Internal service for authenticating.
    """

    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self):
        return self._make_api_call(annon=False)

    @classmethod
    def initialize(cls, loader, endpoint_name, is_secure,
                   endpoint_url, access_token):
        service_name = 'authenticator'
        endpoint_data = loader.load_data('endpoints')
        endpoint_resolver = EndpointResolver(endpoint_data)
        endpoint_bridge = ClientEndpointBridge(endpoint_resolver)
        args_creator = ClientArgsCreator(loader)
        args = args_creator.get_client_args(
            service_name, endpoint_name, endpoint_bridge, is_secure,
            endpoint_url, access_token)
        return cls(**args)
