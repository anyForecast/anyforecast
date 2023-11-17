from fastapi.testclient import TestClient

from anyforecast.web.app import webapp

client = TestClient(webapp.fastapi)


def test_get_info():
    response = client.get("/info")
    data = response.json()

    assert response.status_code == 200
    assert data == {
        "name": "anyforecast",
        "author": "ramonamezquita",
        "email": "contact@anyforecast.com",
    }


def test_add_numbers():
    json = {"x": 5, "y": 5}
    response = client.post("/tasks/add", json=json)
    result = response.json()

    assert response.status_code == 200
    assert result == 10
