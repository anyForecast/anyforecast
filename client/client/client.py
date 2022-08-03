from .args import ClientArgsCreator
from .endpoint import ClientEndpointBridge
from .exceptions import UnknownServiceError
from .services import (
    TrainerService,
    LoginService,
    WriterService,
    PredictionService
)

SERVICES = {
    'writer': WriterService,
    'trainer': TrainerService,
    'prediction': PredictionService,
    'login': LoginService
}


class ClientCreator:
    """Service client creator.

    Parameters
    ----------
    endpoint_resolver : botocore.session.EndpointResolver
        Resolver for endpoint data.
    """

    def __init__(self, loader, endpoint_resolver):
        self._loader = loader
        self._endpoint_resolver = endpoint_resolver

    def create_client(self, service_name, endpoint_name, is_secure=True,
                      endpoint_url=None, access_token=None):
        endpoint_bridge = ClientEndpointBridge(self._endpoint_resolver)
        cls = self._get_client_class(service_name)
        client_args = self._get_client_args(service_name, endpoint_name,
                                            endpoint_bridge, is_secure,
                                            endpoint_url, access_token)
        service_client = cls(**client_args)
        return service_client

    def _get_client_class(self, service_name):
        if service_name not in SERVICES:
            raise UnknownServiceError(name=service_name)
        return SERVICES[service_name]

    def _get_client_args(self, service_name, endpoint_name, endpoint_bridge,
                         is_secure, endpoint_url, access_token):
        args_creator = ClientArgsCreator(self._loader)
        return args_creator.get_client_args(
            service_name, endpoint_name, endpoint_bridge, is_secure,
            endpoint_url, access_token)
