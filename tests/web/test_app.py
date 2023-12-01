from fastapi.testclient import TestClient

from anyforecast.web.app import app

client = TestClient(app)


def test_get_info():
    response = client.get("/info")
    data = response.json()

    assert response.status_code == 200
    assert data == {
        "name": "anyforecast",
        "author": "ramonamezquita",
        "email": "contact@anyforecast.com",
    }


def test_get_root():
    response = client.get("/")
    data = response.json()

    assert response.status_code == 200
    assert data == {"message": "Hello World"}
