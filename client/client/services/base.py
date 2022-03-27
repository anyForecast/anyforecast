from ._minio import MinioClient


class BaseService:
    def __init__(self, endpoint, loader, credentials, s3_endpoint):
        self._endpoint = endpoint
        self._loader = loader
        self._credentials = credentials
        self._minio_client = MinioClient(s3_endpoint, loader, credentials)

    def _make_api_call(self, api_params):
        request_dict = self._convert_to_request_dict(api_params)
        http, parsed_response = self._make_request(request_dict)
        return http
        if http.status_code >= 300:
            error_code = parsed_response.get("Error", {}).get("Code")
            error_class = self.exceptions.from_code(error_code)
            raise error_class(parsed_response)
        else:
            return parsed_response

    def _make_request(self, request_dict):
        try:
            return self._endpoint.make_request(request_dict)
        except Exception as e:
            raise

    def _convert_to_request_dict(self, api_params):
        # request_dict = self._serializer.serialize_to_request(api_params)
        request_dict = api_params
        return request_dict
