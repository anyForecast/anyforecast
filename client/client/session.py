import copy

from .client import ClientCreator, SERVICES
from .configprovider import DEFAULT_SESSION_VARIABLES
from .credentials import Credentials, create_credential_resolver
from .endpoint import EndpointResolver
from .loaders import create_loader


class Session:
    """A session stores configuration state and allows you to create service
    clients.

    It also collects together useful functionality as well as
    important data such as configuration information and credentials into a
    single, easy-to-use object.
    """
    # TODO: support for ``session_vars`` parameter.
    # Remainder: internally, a Session is a collection of components.
    SESSION_VARIABLES = copy.copy(DEFAULT_SESSION_VARIABLES)

    def __init__(self):
        self._credentials = None
        self._access_token = None
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

    def set_access_token(self, access_token):
        self._access_token = access_token

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
                      endpoint_url=None, access_token=None,
                      is_secure=True):
        """Creates service client by name.

        Parameters
        ----------
        service_name : str
            Name of the service. To list available services call method
            :meth:`get_available_services`.

        endpoint_name : str
            Endpoint for the passed service.

        endpoint_url : str, default=None
            The complete URL to use for the constructed
            client.  Normally, botocore will automatically construct the
            appropriate URL to use when communicating with a service.  You can
            specify a complete URL (including the "http/https" scheme) to
            override this behavior.

        access_token : str, default=None
            The access token key to use when creating the client. This is
            entirely optional, and if not provided, the access token configured
            for the session will automatically be used.  You only need to
            provide this argument if you want to override the access token used
            for this specific client.

        is_secure : bool, default=True
            Whether or not to use SSL.  By default, SSL is used.
            Note that not all services support non-ssl connections.
        """
        # if access_key_id is not None and secret_access_key is not None:
        #    credentials = Credentials(
        #        access_key=access_key_id,
        #        secret_key=secret_access_key,
        #        token=session_token)
        # elif self._missing_cred_vars(access_key_id,
        #                             secret_access_key):
        #    raise PartialCredentialsError(
        #        provider='explicit',
        #        cred_var=self._missing_cred_vars(access_key_id,
        #                                         secret_access_key))
        # else:
        #    credentials = self.get_credentials()
        if access_token is None:
            access_token = self.get_access_token()

        loader = self.get_component('data_loader')
        endpoint_resolver = self._get_internal_component('endpoint_resolver')
        client_creator = ClientCreator(loader, endpoint_resolver)
        return client_creator.create_client(service_name, endpoint_name,
                                            is_secure=is_secure,
                                            endpoint_url=endpoint_url,
                                            access_token=access_token)

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

    def get_access_token(self):
        return self._access_token

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
