from requests import Session


class LiveServerSession(Session):
    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs

    def request(self, method, url):
        return super().request(method, url, **self.kwargs)
