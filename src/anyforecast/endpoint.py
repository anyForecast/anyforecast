from requests import Response, Session


class Endpoint:
    """Represents an endpoint for a particular service. Only an endpoint can
    make requests.
    """

    def __init__(
        self,
        url: str,
        endpoint_prefix: str,
        session: Session | None =None,
    ):
        self.url = url
        self.endpoint_prefix = endpoint_prefix
        self.session = session or Session()

    def make_request(self, json=None, data=None, headers=None) -> Response:
        response = self.session.post(
            self.url, json=json, data=data, headers=headers
        )
        return response

    def __repr__(self):
        return "%s(%s)" % (self.endpoint_prefix, self.url)
