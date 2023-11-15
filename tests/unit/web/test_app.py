from fastapi.testclient import TestClient

from anyforecast.web.app import webapp

client = TestClient(webapp.fastapi)


def test_app():
    response = client.get("/info")
    data = response.json()

    assert response.status_code == 200
    assert data == {
        "name": "anyforecast",
        "author": "ramonamezquita",
        "email": "contact@anyforecast.com",
    }
