from .endpoint import EndpointCreator
from .exceptions import UnknownServiceError
from .services import Forecast

SERVICES = {
    'forecast': Forecast
}


class ClientCreator:
    """Creates service clients.
    """

    def __init__(self, loader, endpoint_resolver):
        self._loader = loader
        self._endpoint_resolver = endpoint_resolver

    def create_client(self, service_name, is_secure=True, endpoint_url=None,
                      credentials=None):
        endpoint_bridge = ClientEndpointBridge(self._endpoint_resolver)
        cls = self._get_client_class(service_name)
        client_args = self._get_client_args(service_name, endpoint_bridge,
                                            is_secure, endpoint_url,
                                            credentials)
        service_client = cls(**client_args)
        return service_client

    def _get_client_class(self, service_name):
        if service_name not in SERVICES:
            raise UnknownServiceError(name=service_name)
        return SERVICES[service_name]

    def _get_client_args(self, service_name, endpoint_bridge, is_secure,
                         endpoint_url, credentials):
        args_creator = ClientArgsCreator(self._loader)
        return args_creator.get_client_args(
            service_name, is_secure, endpoint_url,
            credentials, endpoint_bridge
        )


class ClientArgsCreator:
    def __init__(self, loader):
        self._loader = loader

    def get_client_args(self, service_name, is_secure, endpoint_url,
                        credentials, endpoint_bridge):
        final_args = self.compute_client_args(service_name, endpoint_bridge,
                                              endpoint_url, is_secure)

        # Every client requires two endpoints: one for the actual service and
        # the other for s3 (minio).
        service_endpoint = self._get_endpoint(
            final_args['service_endpoint_config']
        )
        s3_endpoint = self._get_endpoint(final_args['s3_endpoint_config'])

        return {
            'endpoint': service_endpoint,
            'loader': self._loader,
            'credentials': credentials,
            's3_endpoint': s3_endpoint
        }

    def _get_endpoint(self, endpoint_config):
        endpoint_creator = EndpointCreator()
        service_name = endpoint_config['service_name']
        endpoint = endpoint_creator.create_endpoint(
            service_name,
            endpoint_url=endpoint_config['endpoint_url']
        )
        return endpoint

    def compute_client_args(self, service_name, endpoint_bridge,
                            endpoint_url, is_secure):
        service_endpoint_config = self._compute_endpoint_config(
            service_name=service_name,
            endpoint_url=endpoint_url,
            is_secure=is_secure,
            endpoint_bridge=endpoint_bridge
        )
        s3_endpoint_config = self._compute_endpoint_config(
            service_name='minio',
            endpoint_url=None,
            is_secure=False,
            endpoint_bridge=endpoint_bridge
        )
        return {
            'service_name': service_name,
            'service_endpoint_config': service_endpoint_config,
            's3_endpoint_config': s3_endpoint_config
        }

    def _compute_endpoint_config(self, service_name, endpoint_url, is_secure,
                                 endpoint_bridge):
        resolve_endpoint_kwargs = {
            'service_name': service_name,
            'endpoint_url': endpoint_url,
            'is_secure': is_secure,
            'endpoint_bridge': endpoint_bridge,
        }
        return self._resolve_endpoint(**resolve_endpoint_kwargs)

    def _resolve_endpoint(self, service_name, endpoint_url, is_secure,
                          endpoint_bridge):
        return endpoint_bridge.resolve(
            service_name=service_name,
            endpoint_url=endpoint_url,
            is_secure=is_secure
        )


class ClientEndpointBridge:
    """Bridges endpoint data and client creation.
    """

    DEFAULT_ENDPOINT = '{service}.{region}.amazonaws.com'

    def __init__(self, endpoint_resolver):
        self.endpoint_resolver = endpoint_resolver

    def resolve(self, service_name, endpoint_url=None, is_secure=True):
        resolved = self.endpoint_resolver.construct_endpoint(service_name)
        if resolved:
            endpoint_name = resolved.get('endpointName')
            return self._create_endpoint(resolved, service_name,
                                         endpoint_name, endpoint_url,
                                         is_secure)

    def _create_endpoint(self, resolved, service_name, endpoint_name,
                         endpoint_url,
                         is_secure):
        if endpoint_url is None:
            hostname = resolved.get('hostname')
            endpoint_url = self._make_url(
                hostname,
                is_secure,
                resolved.get('protocols', [])
            )
        return self._create_result(service_name, endpoint_name, endpoint_url,
                                   resolved)

    def _create_result(self, service_name, endpoint_name, endpoint_url,
                       metadata):
        return {
            'service_name': service_name,
            'endpoint_name': endpoint_name,
            'endpoint_url': endpoint_url,
            'metadata': metadata
        }

    def _make_url(self, hostname, is_secure, supported_protocols):
        if is_secure and 'https' in supported_protocols:
            scheme = 'https'
        else:
            scheme = 'http'
        return '%s://%s' % (scheme, hostname)
