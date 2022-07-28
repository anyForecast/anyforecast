import http
from abc import ABCMeta, abstractmethod


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = None) -> None:
        if detail is None:
            detail = http.HTTPStatus(status_code).phrase
        self.status_code = status_code
        self.detail = detail

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(status_code={self.status_code}, detail={self.detail})"


class BaseService(metaclass=ABCMeta):
    def __init__(self, endpoint, loader, access_token):
        self._endpoint = endpoint
        self._loader = loader
        self._access_token = access_token

    @abstractmethod
    def post(self, *args, **kwargs):
        pass

    def get_endpoint(self):
        return self._endpoint

    def _make_api_call(self, json=None, data=None, annon=False):
        headers = self._create_headers(annon)
        http_response = self._make_request(json, data, headers)
        parsed_response = http_response.json()
        if http_response.status_code >= 300:
            detail = parsed_response.get("detail", {})
            status_code = http_response.status_code
            raise HTTPException(status_code, detail)
        else:
            return parsed_response

    def _make_request(self, json, data, headers):
        try:
            return self._endpoint.make_request(json, data, headers)
        except Exception as e:
            raise

    def _create_headers(self, annon):
        if annon:
            return None
        auth_bearer = self._create_auth_bearer()
        return {'accept': 'application/json', 'Authorization': auth_bearer}

    def _create_auth_bearer(self):
        return 'Bearer ' + self._access_token

    # def _convert_to_request_dict(self, api_params, annon):
    # request_dict = self._serializer.serialize_to_request(api_params)
    # return request_dict
