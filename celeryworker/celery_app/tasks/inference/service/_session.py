import http
import json
import os
from collections import OrderedDict
from typing import Dict, OrderedDict, Union

import pandas as pd

from ._endpoint import Endpoint
from ...serialize import Serializer, PandasSerializer


class InferenceSession:
    def __init__(self):
        self._components = ComponentLocator()
        self._register_components()

    def _register_components(self):
        self._register_endpoint_resolver()

    def _register_endpoint_resolver(self):
        def create_default_resolver():
            loader = JSONFileLoader()
            endpoints = loader.load_file('data/endpoints')
            return EndpointResolver(endpoints)

        self._components.lazy_register_component(
            'endpoint_resolver', create_default_resolver)

    def get_component(self, name):
        return self._components.get_component(name)

    def create_client(self, service_name, model_name):
        endpoint_resolver = self.get_component('endpoint_resolver')
        client_creator = InferenceClientCreator(endpoint_resolver)
        return client_creator.create_client(service_name, model_name)


class InferenceClient:

    def __init__(
            self, endpoint: Endpoint,
            serializer: Serializer = PandasSerializer()
    ):
        self._endpoint = endpoint
        self._serializer = serializer

    def make_inference(self, X: pd.DataFrame):
        self._make_api_call(X)

    def _make_api_call(self, X):

        X = self._serialize(X)
        headers = self._create_headers()
        http_response = self._make_request(X, None, headers)
        parsed_response = http_response.json()

        if http_response.status_code >= 300:
            detail = parsed_response.get("message", {})
            status_code = http_response.status_code
            raise HTTPException(status_code, detail)
        else:
            return parsed_response

    def _serialize(self, X):
        return self._serializer.serialize(X)

    def _create_headers(self):
        app_or_json = 'application/json'
        return {'accept': app_or_json, 'Content-Type': app_or_json}

    def _make_request(self, json, data, headers):
        try:
            return self._endpoint.make_request(json, data, headers)
        except Exception as e:
            raise


class EndpointResolver:
    """Provides object oriented access to endpoint metadata.

    Parameters
    ----------
    endpoint_data : dict
        A dict of endpoint configuration.
    """
    def __init__(self, endpoint_data: Union[Dict, OrderedDict]):
        self.endpoint_data = endpoint_data

    def resolve(self, service_name: str, model_name: str):
        """Resolves endpoint data.

        Parameters
        ----------
        service_name : str
            Service name.

        model_name : str
            Model name

        Returns
        -------
        resolution : OrderedDict
        """

        service_data = self._get_service_data(service_name)
        endpoint_name = service_data.get('endpoint', '')
        port = service_data.get('port', '')
        hostname = self._get_hostname(service_data)
        result = service_data
        result['model_name'] = model_name
        result['hostname'] = self._expand_template(
            hostname, service_name, model_name, endpoint_name, port)

        return result

    def _get_service_data(self, service_name):
        try:
            return self.endpoint_data['services'][service_name]
        except KeyError:
            raise

    def _get_hostname(self, service_data):
        default_hostname = self.endpoint_data['defaults']['hostname']
        return service_data.get('hostname', default_hostname)

    def _expand_template(self, template, service_name, model_name,
                         endpoint_name, port):
        return template.format(
            service=service_name, model=model_name, endpoint=endpoint_name,
            port=port)


class InferenceClientCreator:
    """Creates :class:`InferenceClient` instances.

    Parameters
    ----------
    endpoint_resolver : EndpointResolver
    """
    def __init__(self, endpoint_resolver: EndpointResolver):
        self.endpoint_resolver = endpoint_resolver


    def _bridge_client_endpoint(self, service_name, model_name):
        resolved = self.endpoint_resolver.resolve(service_name, model_name)



    def _create_endpoint(self, service_name, model_name):
        resolved = self.endpoint_resolver.resolve(service_name, model_name)

        EndpointCreator()

    def create_client(self, service_name, model_name, is_secure=True):
        return InferenceClient()


class JSONFileLoader:
    """Loader JSON files.
    """

    def exists(self, file_path):
        """Checks if the file exists.

        Parameters
        ----------
        file_path: str
            The full path to the file to load without the '.json' extension.

        Returns
        -------
        True if file path exists, False otherwise.
        """
        return os.path.isfile(file_path + '.json')

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


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = None) -> None:
        if detail is None:
            detail = http.HTTPStatus(status_code).phrase
        self.status_code = status_code
        self.detail = detail

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(status_code={self.status_code}, detail={self.detail})"
