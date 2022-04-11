"""Internal module to help with normalizing botocore client args.

This module (and all function/classes within this module) should be
considered internal, and *not* a public API.
"""
from .endpoint import EndpointCreator


class ClientArgsCreator:
    """Creates client args.

    Since services share a common interface, a consistent procedure for
    obtaining their constructor arguments can be achieved.
    """

    def get_client_args(self, service_name, endpoint_name, is_secure,
                        endpoint_url, credentials, endpoint_bridge):
        # Resolve endpoint data.
        resolved = self._resolve_endpoint(
            service_name, endpoint_name, endpoint_url, is_secure,
            endpoint_bridge)

        # Create endpoint object.
        endpoint_config = resolved['endpoint_config']
        endpoint = self._get_endpoint(endpoint_config)

        return {'endpoint': endpoint, 'credentials': credentials}

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
