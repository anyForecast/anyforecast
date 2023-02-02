import http

import pandas as pd

from ._endpoint import Endpoint, EndpointCreator, EndpointResolver
from ...serializers import PandasSerializer, DataFrameSerializer


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = None) -> None:
        if detail is None:
            detail = http.HTTPStatus(status_code).phrase
        self.status_code = status_code
        self.detail = detail

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(status_code={self.status_code}, detail={self.detail})"


class InferenceClientCreator:
    """Creates :class:`InferenceClient` instances.

    Parameters
    ----------
    endpoint_resolver : EndpointResolver
    """

    def __init__(self, endpoint_resolver: EndpointResolver):
        self.endpoint_resolver = endpoint_resolver

    def create_client(
            self, service_name: str, model_name: str, is_secure: bool = True,
            serializer: DataFrameSerializer = PandasSerializer()
    ):
        endpoint = self._create_endpoint(service_name, model_name, is_secure)
        return InferenceClient(endpoint, serializer)

    def _create_endpoint(self, service_name, model_name, is_secure):
        resolved = self.endpoint_resolver.resolve(service_name, model_name)
        endpoint_url = self._make_url(resolved['hostname'], is_secure)
        return EndpointCreator().create_endpoint(service_name, endpoint_url)

    def _make_url(self, hostname, is_secure):
        if is_secure:
            scheme = 'https'
        else:
            scheme = 'http'
        return '%s://%s' % (scheme, hostname)


class InferenceClient:

    def __init__(self, endpoint: Endpoint, serializer: DataFrameSerializer):
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
