"""Internal module to help with normalizing client args.

This module (and all function/classes within this module) should be
considered internal, and *not* a public API.
"""
from abc import abstractmethod, ABCMeta

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
            is_secure=is_secure)


class CloudArgsCreator(metaclass=ABCMeta):
    """Base abstract class for cloud args creator.

    Derived classes are not meant to be constructed
    directly. Instead, instances of derived classes are constructed and
    returned from :meth:`make_args_creator`.
    """

    def __init__(self, credentials, endpoint=None):
        self._credentials = credentials
        self._endpoint = endpoint

    @abstractmethod
    def create_args(self):
        pass

    @classmethod
    def make_args_creator(cls, name, credentials, endpoint=None):
        """Factory function for :class:`CloudArgsCreator` objects.

        Parameters
        ----------
        name : str
            Args creator name.

        credentials : client.credentials.Credentials
            Client credentials object.

        endpoint : client.endpoint.Endpoint
            Endpoint object.

        Returns
        -------
        cloud_args_creator : CloudArgsCreator
        """
        args_creators = {
            'minio': MinioArgsCreator,
            'S3FileSystem': S3FilesystemArgsCreator
        }
        return args_creators[name](credentials, endpoint)

    def _split_host(self):
        protocol, endpoint = self._endpoint.host.split('://')
        return protocol, endpoint

    def _is_secure(self):
        protocol, _ = self._split_host()
        if protocol == 'https':
            return True
        elif protocol == 'http':
            return False
        else:
            raise ValueError(f'Invalid protocol: {protocol}')


class MinioArgsCreator(CloudArgsCreator):
    def __init__(self, credentials, endpoint=None):
        super().__init__(credentials, endpoint)

    def create_args(self):
        if hasattr(self._credentials, 's3_endpoint'):
            endpoint = self._credentials.s3_endpoint
        else:
            _, endpoint = self._split_host()
        return {
            "endpoint": endpoint,
            "access_key": self._credentials.access_key,
            "secret_key": self._credentials.secret_key,
            "secure": self._is_secure()
        }


class S3FilesystemArgsCreator(CloudArgsCreator):
    def __init__(self, credentials, endpoint=None):
        super().__init__(credentials, endpoint)

    def create_args(self):
        return {
            "endpoint_url": self._endpoint.host,
            "aws_access_key_id": self._credentials.access_key,
            "aws_secret_access_key": self._credentials.secret_key,
            "verify": False
        }
