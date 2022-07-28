from .base import BaseService


class LoginService(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def post(self, username, password):
        api_params = {
            'username': username,
            'password': password,
        }

        # Notice ``annon`` param is set to True.
        return self._make_api_call(data=api_params, annon=True)
