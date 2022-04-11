import copy
import logging

from .client import ClientCreator, SERVICES
from .configprovider import DEFAULT_SESSION_VARIABLES
from .credentials import Credentials, create_credential_resolver
from .exceptions import (PartialCredentialsError, NoRegionError,
                         InvalidEndpointForService)
from .loaders import create_loader


class Session:
    """The Session object collects together useful functionality as well as
    important data such as configuration information and credentials into a
    single, easy-to-use object.
    """
    # TODO: support for ``session_vars`` parameter.
    # Remainder: a Session is a collection of components.
    SERVICES = ['forecast']
    SESSION_VARIABLES = copy.copy(DEFAULT_SESSION_VARIABLES)

    def __init__(self):
        self._credentials = None
        self._components = ComponentLocator()
        self._internal_components = ComponentLocator()
        self._register_components()

    def _register_credential_provider(self):
        self._components.lazy_register_component(
            'credential_provider', create_credential_resolver)

    def _register_data_loader(self):
        self._components.lazy_register_component(
            'data_loader', create_loader)

    def _register_endpoint_resolver(self):
        def create_default_resolver():
            loader = self.get_component('data_loader')
            endpoints = loader.load_data('endpoints')
            return EndpointResolver(endpoints)

        self._internal_components.lazy_register_component(
            'endpoint_resolver', create_default_resolver)

    def _register_components(self):
        self._register_credential_provider()
        self._register_data_loader()
        self._register_endpoint_resolver()

    def set_credentials(self, access_key, secret_key, token=None):
        """Manually create credentials for this session.

        Parameters
        ----------
        access_key : str
            The access key part of the credentials.

        secret_key : str
            The secret key part of the credentials.

        token : str
            An option session token used by STS session credentials.
        """
        self._credentials = Credentials(access_key, secret_key, token)

    def create_client(self, service_name, endpoint_name=None,
                      endpoint_url=None, access_key_id=None,
                      secret_access_key=None, session_token=None,
                      is_secure=True):
        """Create a low-level service client by name.

        Parameters
        ----------
        service_name : str
            The name of a service, e.g. 'forecast' or 'another_service'. You
            can get a list of available services via
            :py:meth:`get_available_services`.

        endpoint_url : str, default=None
            The complete URL to use for the constructed
            client.  Normally, botocore will automatically construct the
            appropriate URL to use when communicating with a service.  You can
            specify a complete URL (including the "http/https" scheme) to
            override this behavior.

        access_key_id : str, default=None
            The access key to use when creating the client. This is entirely
            optional, and if not provided, the credentials configured for the
            session will automatically be used.  You only need to provide this
            argument if you want to override the credentials used for this
            specific client.

        secret_access_key : str, default=None
            The access key to use when creating the client.
            Same semantics as access_key_id above.

        session_token : str, default=None
            The session token to use when creating
            the client.  Same semantics as aws_access_key_id above.
        """
        loader = self.get_component('data_loader')
        if access_key_id is not None and secret_access_key is not None:
            credentials = Credentials(
                access_key=access_key_id,
                secret_key=secret_access_key,
                token=session_token)
        elif self._missing_cred_vars(access_key_id,
                                     secret_access_key):
            raise PartialCredentialsError(
                provider='explicit',
                cred_var=self._missing_cred_vars(access_key_id,
                                                 secret_access_key))
        else:
            credentials = self.get_credentials()

        endpoint_resolver = self._get_internal_component('endpoint_resolver')
        client_creator = ClientCreator(loader, endpoint_resolver)
        return client_creator.create_client(service_name, endpoint_name,
                                            is_secure=is_secure,
                                            endpoint_url=endpoint_url,
                                            credentials=credentials)

    def get_component(self, name):
        return self._components.get_component(name)

    def _get_internal_component(self, name):
        # While this method may be called by botocore classes outside of the
        # Session, this method should **never** be used by a class that lives
        # outside of botocore.
        return self._internal_components.get_component(name)

    def _missing_cred_vars(self, access_key, secret_key):
        if access_key is not None and secret_key is None:
            return 'secret_access_key'
        if secret_key is not None and access_key is None:
            return 'access_key_id'
        return None

    def get_credentials(self):
        """Returns the :class:`botocore.credential.Credential` object
        associated with this session.  If the credentials have not
        yet been loaded, this will attempt to load them.  If they
        have already been loaded, this will return the cached
        credentials.
        """
        if self._credentials is None:
            self._credentials = self._components.get_component(
                'credential_provider').load_credentials()
        return self._credentials

    def get_available_services(self):
        """Get a list of available services that can be loaded as low-level
        clients via :py:meth:`Session.client`.

        Returns
        -------
        services : list
            List of services names
        """
        return list(SERVICES)


class ComponentLocator:
    """Service locator for session components."""

    def __init__(self):
        self._components = {}
        self._deferred = {}

    def get_component(self, name):
        if name in self._deferred:
            factory = self._deferred[name]
            self._components[name] = factory()
            # Only delete the component from the deferred dict after
            # successfully creating the object from the factory as well as
            # injecting the instantiated value into the _components dict.
            del self._deferred[name]
        try:
            return self._components[name]
        except KeyError:
            raise ValueError("Unknown component: %s" % name)

    def register_component(self, name, component):
        self._components[name] = component
        try:
            del self._deferred[name]
        except KeyError:
            pass

    def lazy_register_component(self, name, no_arg_factory):
        self._deferred[name] = no_arg_factory
        try:
            del self._components[name]
        except KeyError:
            pass


LOG = logging.getLogger(__name__)
DEFAULT_URI_TEMPLATE = '{service}:{port}/{endpoint}.{dnsSuffix}'
DEFAULT_SERVICE_DATA = {'endpoints': {}}


class EndpointResolver:
    """Resolves endpoints based on endpoint metadata.

    Parameters
    ----------
    endpoint_data : dict
        A dict of endpoint configuration.
    """

    def __init__(self, endpoint_data):
        if 'partitions' not in endpoint_data:
            raise ValueError('Missing "partitions" in endpoint data')
        self._endpoint_data = endpoint_data

    def construct_endpoint(self, service_name, endpoint_name=None,
                           partition_name=None):

        """Constructs endpoint data.

        Parameters
        ----------
        service_name : str
        endpoint_name : name
        partition_name : name

        Returns
        -------
        endpoint_data : dict
        """
        if partition_name is not None:
            valid_partition = None
            for partition in self._endpoint_data['partitions']:
                if partition['partition'] == partition_name:
                    valid_partition = partition
            if valid_partition is not None:
                result = self._endpoint_for_partition(
                    valid_partition, service_name, endpoint_name)
                return result
            return None

        # Iterate over each partition until a match is found.
        for partition in self._endpoint_data['partitions']:
            result = self._endpoint_for_partition(
                partition, service_name, endpoint_name)
            if result:
                return result

    def get_available_endpoints(self):
        """Obtains collection of available endpoints.

        Returns
        -------
        endpoints : list
            List of endpoints.
        """
        return self._endpoint_data['endpoints']

    def _get_endpoint_name(self, service_data, service_name, endpoint_name):
        # Use the partition endpoint if no region is supplied.
        if endpoint_name is None:
            if 'partitionEndpoint' in service_data:
                endpoint_name = service_data['partitionEndpoint']
            elif not service_data['endpoints']:
                endpoint_name = ''
            else:
                raise NoRegionError()
        else:
            # Attempt to resolve the exact endpoint name.
            if endpoint_name not in service_data['endpoints']:
                raise InvalidEndpointForService(endpoint=endpoint_name,
                                                service=service_name)

        return endpoint_name

    def _endpoint_for_partition(self, partition, service_name, endpoint_name):
        # Get the service from the partition, or an empty template.
        service_data = partition['services'].get(
            service_name, DEFAULT_SERVICE_DATA)

        endpoint_name = self._get_endpoint_name(service_data, service_name,
                                                endpoint_name)

        resolve_kwargs = {
            'partition': partition,
            'service_name': service_name,
            'service_data': service_data,
            'endpoint_name': endpoint_name
        }
        return self._resolve(**resolve_kwargs)

    def _resolve(self, partition, service_name, service_data, endpoint_name):
        endpoint_data = service_data.get("endpoints", {}).get(endpoint_name,
                                                              {})
        if endpoint_data.get('deprecated'):
            LOG.warning(
                'Client is configured with the deprecated endpoint: %s' % (
                    endpoint_name
                )
            )

        service_defaults = service_data.get('defaults', {})
        partition_defaults = partition.get('defaults', {})
        result = endpoint_data

        # If a port is included in service_data
        port = service_data.get('port', '')

        # If dnsSuffix has not already been consumed from a variant definition
        if 'dnsSuffix' not in result:
            result['dnsSuffix'] = partition['dnsSuffix']

        # Insert names in resulting dict.
        result['partition'] = partition['partition']
        result['endpointName'] = endpoint_name

        # Merge in the service defaults then the partition defaults.
        self._merge_keys(service_defaults, result)
        self._merge_keys(partition_defaults, result)

        result['hostname'] = self._expand_template(
            partition, result['hostname'], service_name, endpoint_name,
            result['dnsSuffix'], port
        )

        return result

    def _merge_keys(self, from_data, result):
        for key in from_data:
            if key not in result:
                result[key] = from_data[key]

    def _expand_template(self, partition, template, service_name,
                         endpoint_name, dnsSuffix, port):
        return template.format(
            service=service_name, endpoint=endpoint_name,
            dnsSuffix=dnsSuffix, port=port)

    def get_service_endpoints_data(self, service_name, partition_name='aws'):
        """Obtains endpoint data for a particular partition-service
        combination.

        Parameters
        ----------
        service_name : str
            Service name inside partition.

        partition_name : str, default='aws'
            Partition name.
        """
        for partition in self._endpoint_data['partitions']:
            if partition['partition'] != partition_name:
                continue
            services = partition['services']
            if service_name not in services:
                continue
            return services[service_name]['endpoints']

    def get_available_partitions(self):
        """Obtains collection of available partitions.

        Returns
        -------
        partitions : list
            List with partition names.
        """
        result = []
        for partition in self._endpoint_data['partitions']:
            result.append(partition['partition'])
        return result
