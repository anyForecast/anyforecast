"""Internal module to help with normalizing botocore client args.

This module (and all function/classes within this module) should be
considered internal, and *not* a public API.
"""
from .endpoint import EndpointCreator


class ClientArgsCreator:
    """Service client args creator.

    Since services share a common interface, a consistent procedure for
    obtaining their constructor arguments can be achieved.
    """

    def __init__(self, loader):
        self._loader = loader

    def get_client_args(self, service_name, endpoint_name, endpoint_bridge,
                        is_secure, endpoint_url, access_token):
        """Obtains client args.

        Parameters
        ----------
        service_name : str
            Name of the service. To list available services call method
            :meth:`get_available_services`.

        endpoint_name : str
            Endpoint for the passed service.

        endpoint_bridge : botocore.client.EndpointBridge
            EndpointBridge object for resolving the endpoint.

        is_secure : bool
            Whether or not to use SSL.  By default, SSL is used.
            Note that not all services support non-ssl connections.

        endpoint_url : str
            The complete URL to use for the constructed
            client.

        access_token : str
            Access token for authenticating.

        Returns
        -------
        args : dict
        """
        # Resolve endpoint data.
        resolved = self._resolve_endpoint(
            service_name, endpoint_name, endpoint_url, is_secure,
            endpoint_bridge)

        # Create endpoint object.
        endpoint = self._get_endpoint(resolved)

        return {
            'endpoint': endpoint,
            'loader': self._loader,
            'access_token': access_token
        }

    def _get_endpoint(self, endpoint_config):
        endpoint_creator = EndpointCreator()
        service_name = endpoint_config['service_name']
        endpoint = endpoint_creator.create_endpoint(
            service_name,
            endpoint_url=endpoint_config['endpoint_url']
        )
        return endpoint

    def _resolve_endpoint(self, service_name, endpoint_name, endpoint_url,
                          is_secure, endpoint_bridge):
        return endpoint_bridge.resolve(
            service_name=service_name,
            endpoint_name=endpoint_name,
            endpoint_url=endpoint_url,
            is_secure=is_secure
        )
