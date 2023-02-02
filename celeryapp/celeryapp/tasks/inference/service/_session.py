import json
import os
from collections import OrderedDict
from typing import OrderedDict

from . import API_ROOT
from ._client import InferenceClientCreator
from ._endpoint import EndpointResolver
from ...exceptions import DataNotFoundError


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

    def create_client(self, service_name: str, model_name: str):
        endpoint_resolver = self.get_component('endpoint_resolver')
        client_creator = InferenceClientCreator(endpoint_resolver)
        return client_creator.create_client(service_name, model_name)


class JSONFileLoader:
    """Loader JSON files.
    """

    BUILTIN_DATA_PATH = os.path.join(API_ROOT, 'data')

    def __init__(
            self, extra_search_paths=None, include_default_search_paths=True
    ):

        if extra_search_paths is not None:
            self._search_paths = extra_search_paths
        else:
            self._search_paths = []

        if include_default_search_paths:
            self._search_paths.extend([self.BUILTIN_DATA_PATH])

    @property
    def search_paths(self):
        return self._search_paths

    def load_data(self, name):
        """Load data given a data path.

        This is a low level method that will search through the various
        search paths until it's able to load a value.

        Parameters
        ----------
        name : str
            The data path, i.e ``ec2/2015-03-01/service-2``.

        Raises
        ------
        DataNotFoundError if no data could be found.

        Returns
        -------
        data : dict
            The loaded data.
        """
        for possible_path in self._potential_locations(name):
            found = self.load_file(possible_path)
            if found is not None:
                return found
        # We didn't find anything that matched on any path.
        raise DataNotFoundError(data_path=name)

    def load_file(self, file_path):
        """Attempt to load the file path.

        Parameters
        ----------

        file_path: str
            The full path to the file to load without the '.json' extension.

        Returns
        -------
        file: OrderedDict
            The loaded data if it exists, otherwise None.
        """
        full_path = file_path + '.json'
        if not os.path.isfile(full_path):
            return

        # By default, the file will be opened with locale encoding on Python 3.
        # We specify "utf8" here to ensure the correct behavior.
        with open(full_path, 'rb') as fp:
            payload = fp.read().decode('utf-8')

        return json.loads(payload, object_pairs_hook=OrderedDict)

    def _potential_locations(self, name=None, must_exist=False,
                             is_dir=False):
        # Will give an iterator over the full path of potential locations
        # according to the search path.
        for path in self.search_paths:
            if os.path.isdir(path):
                full_path = path
                if name is not None:
                    full_path = os.path.join(path, name)
                if not must_exist:
                    yield full_path
                else:
                    if is_dir and os.path.isdir(full_path):
                        yield full_path
                    elif os.path.exists(full_path):
                        yield full_path


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
