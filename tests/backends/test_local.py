import time
import unittest
import urllib.request

from anyforecast.backends import LocalBackend

URL = "http://www.foxnews.com/"
SLEEP = 3


class URLLoader:
    """Retrieves a single page and report the URL and contents" """

    def __init__(self, url: str) -> None:
        self.url = url

    def run(self):
        time.sleep(SLEEP)
        with urllib.request.urlopen(url=self.url) as conn:
            return conn.read()


class TestLocalBackend(unittest.TestCase):

    def setUp(self) -> None:
        self.runner = URLLoader(URL)

    def test_is_done(self):
        local_backend = LocalBackend()
        promise = local_backend.run(self.runner)
        promise.result()
        assert promise.done()
