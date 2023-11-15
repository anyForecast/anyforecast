from unittest import TestCase, mock

import requests

from anyforecast.predictor import Predictor

ENDPOINT = "dummy_endpoint"
RETURN_JSON = {}


class PredictorTest(TestCase):
    def setUp(self) -> None:
        mock_post = mock.MagicMock(return_value=RETURN_JSON)
        mock_session = mock.MagicMock(post=mock_post)
        patcher = mock.patch("requests.Session", return_value=mock_session)

        self.MockSession = patcher.start()
        self.addClassCleanup(patcher.stop)

    def test_mock_session(self):
        assert requests.Session is self.MockSession

    def test_predict_call(self):
        mock_session = self.MockSession()
        predictor = Predictor(ENDPOINT, session=mock_session)
        pass
