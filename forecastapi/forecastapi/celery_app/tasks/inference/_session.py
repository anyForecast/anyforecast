import http

from requests import Session as HttpSession


class Endpoint:
    """Represents an endpoint for a particular container.
    Only an endpoint can make requests.
    """

    def __init__(self, host, endpoint_prefix, response_parser_factory=None,
                 http_session=None):
        self._endpoint_prefix = endpoint_prefix
        self.host = host
        if response_parser_factory is None:
            pass
            # response_parser_factory = parsers.ResponseParserFactory()
        self._response_parser_factory = response_parser_factory
        self.http_session = http_session
        if self.http_session is None:
            self.http_session = HttpSession()

    def make_request(self, json=None, data=None, headers=None):
        http_response = self.http_session.post(
            url=self.host, json=json, data=data, headers=headers)
        return http_response

    def __repr__(self):
        return '%s(%s)' % (self._endpoint_prefix, self.host)


class EndpointCreator:
    DEFAULT_ENDPOINT_DATA = {'endpoint': 'invocations', 'port': '8080'}
    TEMPLATE = "{model_name}:{port}/{endpoint}"

    def __init__(self):
        pass

    def create_endpoint(self, model_name):
        endpoint_data = self._get_endpoint_data(model_name)
        hostname = self._expand_template(model_name, **endpoint_data)
        endpoint_url = self._make_url(
            hostname=hostname,
            is_secure=False,
            supported_protocols=endpoint_data.get('protocols', [])
        )
        return Endpoint(endpoint_url, model_name)

    def _expand_template(self, model_name, port, endpoint):
        return self.TEMPLATE.format(
            model_name=model_name, port=port, endpoint=endpoint)

    def _make_url(self, hostname, is_secure, supported_protocols):
        if is_secure and 'https' in supported_protocols:
            scheme = 'https'
        else:
            scheme = 'http'
        return '%s://%s' % (scheme, hostname)

    def _get_endpoint_data(self, model_name):
        return self.DEFAULT_ENDPOINT_DATA


class InferenceService:

    def __init__(self, endpoint):
        self._endpoint = endpoint

    def post(self, X):
        X = self._serialize_input(X)

    def _make_api_call(self, json=None, data=None):
        headers = self._create_headers()
        http_response = self._make_request(json, data, headers)
        parsed_response = http_response.json()
        if http_response.status_code >= 300:
            detail = parsed_response.get("message", {})
            status_code = http_response.status_code
            raise HTTPException(status_code, detail)
        else:
            return parsed_response

    def _make_request(self, json, data, headers):
        try:
            return self._endpoint.make_request(json, data, headers)
        except Exception as e:
            raise

    def _create_headers(self):
        app_or_json = 'application/json'
        return {'accept': app_or_json, 'Content-Type': app_or_json}


class InferenceSession:
    def __init__(self):
        pass

    def _create_endpoint(self, model_name):
        endpoint_creator = EndpointCreator()
        return endpoint_creator.create_endpoint(model_name)

    def create_client(self, model_name):
        endpoint = self._create_endpoint(model_name)
        return InferenceService(endpoint)


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = None) -> None:
        if detail is None:
            detail = http.HTTPStatus(status_code).phrase
        self.status_code = status_code
        self.detail = detail

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}(status_code={self.status_code}, detail={self.detail})"
