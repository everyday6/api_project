from unittest.mock import patch

from fastapi.testclient import TestClient

from src.serving.nav_api import app

client = TestClient(app)


def test_get_segment_values_returns_values_in_order():
    with patch("src.serving.nav_api.resolve_segment_values", return_value=[30, 50]) as mock_resolve:
        response = client.post(
            "/segments/values",
            json={"segment_ids": ["1", "2"], "type": 1, "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {"values": [30, 50]}
    mock_resolve.assert_called_once_with(["1", "2"], 1, "12:00")


def test_get_segment_values_rejects_invalid_type():
    response = client.post(
        "/segments/values",
        json={"segment_ids": ["1"], "type": 3, "time": "12:00"},
    )

    assert response.status_code == 422


def test_get_segment_values_rejects_malformed_time():
    response = client.post(
        "/segments/values",
        json={"segment_ids": ["1"], "type": 1, "time": "not-a-time"},
    )

    assert response.status_code == 422


def test_get_segment_values_rejects_empty_segment_list():
    response = client.post(
        "/segments/values",
        json={"segment_ids": [], "type": 1, "time": "12:00"},
    )

    assert response.status_code == 422


def test_get_segment_values_rejects_too_many_segment_ids():
    response = client.post(
        "/segments/values",
        json={"segment_ids": [str(i) for i in range(501)], "type": 1, "time": "12:00"},
    )

    assert response.status_code == 422


def test_health_returns_ok():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
