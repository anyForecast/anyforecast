class BaseService:
    def __init__(self, endpoint, loader, access_token):
        self.endpoint = endpoint
        self.loader = loader
        self.access_token = access_token

    def _make_api_call(self, json=None, data=None, annon=False):
        headers = self._create_headers(annon)
        http = self._make_request(json, data, headers)
        return http
        if http.status_code >= 300:
            error_code = parsed_response.get("Error", {}).get("Code")
            error_class = self.exceptions.from_code(error_code)
            raise error_class(parsed_response)
        else:
            return http

    def _make_request(self, json, data, headers):
        try:
            return self.endpoint.make_request(json, data, headers)
        except Exception as e:
            raise

    def _create_headers(self, annon):
        if annon:
            return None
        auth_bearer = self._create_auth_bearer()
        return {'accept': 'application/json', 'Authorization': auth_bearer}

    def _create_auth_bearer(self):
        return 'Bearer ' + self.access_token

    # def _convert_to_request_dict(self, api_params, annon):
    # request_dict = self._serializer.serialize_to_request(api_params)
    # return request_dict

    def _create_api_params(self, **kwargs):
        return kwargs
