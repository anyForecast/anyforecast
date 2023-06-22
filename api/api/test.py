from fastapi.testclient import TestClient

from .main import app

client = TestClient(app)


def test_app():
    response = client.get("/info")
    data = response.json()
    assert data == {
        "app_name": "anyForecast"
    }