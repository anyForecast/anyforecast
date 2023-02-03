from ._endpoint import EndpointResolver
from ._loaders import JSONFileLoader
from .client import InferenceClientCreator
from ...serializers import DataFrameSerializer


class InferenceSession:
    def __init__(self):
        self._components = ComponentLocator()
        self._register_components()

    def _register_components(self):
        self._register_endpoint_resolver()

    def _register_endpoint_resolver(self):
        def create_default_resolver():
            loader = JSONFileLoader()
            endpoints = loader.load_data('endpoints')
            return EndpointResolver(endpoints)

        self._components.lazy_register_component(
            'endpoint_resolver', create_default_resolver)

    def get_component(self, name):
        return self._components.get_component(name)

    def create_client(
            self,
            service_name: str,
            model_name: str,
            is_secure: bool = True,
            serializer: DataFrameSerializer = None
    ):
        endpoint_resolver = self.get_component('endpoint_resolver')
        client_creator = InferenceClientCreator(endpoint_resolver)
        return client_creator.create_client(
            service_name, model_name, is_secure=is_secure,
            serializer=serializer)


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
