from getpass import getpass

from client.services.base import BaseService


class Login(BaseService):
    def __init__(self, endpoint, loader, access_token):
        super().__init__(endpoint, loader, access_token)

    def login(self):
        username = input('Username: ')
        password = getpass('Password: ')
        api_params = self._create_api_params(
            username=username,
            password=password
        )

        # Notice `annon` param is set to True.
        return self._make_api_call(data=api_params, annon=True)

