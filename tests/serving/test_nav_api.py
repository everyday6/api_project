from unittest.mock import patch

from fastapi.testclient import TestClient

from src.serving.nav_api import app

client = TestClient(app)


_T1_PROV = [
    {"storage_source": "rds", "value_basis": "observed"},
    {"storage_source": "s3_snapshot", "value_basis": "historical_average"},
]


def test_get_segment_values_returns_values_provenance_and_derived_sources():
    with patch(
        "src.serving.nav_api.resolve_segment_values_with_tiers",
        return_value=([30, 50], _T1_PROV),
    ) as mock_resolve:
        response = client.post(
            "/segments/values",
            json={"segment_ids": ["1", "2"], "type": 1, "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "values": [30, 50],
        # sources는 provenance에서 파생 - type1 어휘로 fresh/avg
        "sources": ["fresh", "avg"],
        "provenance": _T1_PROV,
    }
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


def test_navigation_values_type1_dispatches_to_resolve_segment_values():
    with patch(
        "src.serving.nav_api.resolve_segment_values_with_tiers",
        return_value=([30, 50], _T1_PROV),
    ) as mock_resolve:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1", "2"], "type": 1, "date": "2026-08-23", "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "value": [30.0, 50.0], "sources": ["fresh", "avg"], "provenance": _T1_PROV,
    }
    mock_resolve.assert_called_once_with(["1", "2"], 1, "12:00")


def test_navigation_values_type2_dispatches_to_resolve_segment_values():
    prov2 = [{"storage_source": "rds", "value_basis": "segment_value"}]
    with patch(
        "src.serving.nav_api.resolve_segment_values_with_tiers",
        return_value=([100], prov2),
    ) as mock_resolve:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1"], "type": 2, "date": "2026-08-23", "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {"value": [100.0], "sources": ["rds"], "provenance": prov2}
    mock_resolve.assert_called_once_with(["1"], 2, "12:00")


def test_navigation_values_type3_combines_date_and_time_into_datetime():
    from datetime import datetime

    prov3 = [
        {"storage_source": "rds", "value_basis": "modeled_aggregate"},
        {"storage_source": "s3_snapshot", "value_basis": "modeled_aggregate"},
    ]
    with patch(
        "src.serving.nav_api.get_type3_values_with_tiers",
        return_value=([12.5, 7.0], prov3),
    ) as mock_type3:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1", "2"], "type": 3, "date": "2026-08-23", "time": "14:30"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "value": [12.5, 7.0], "sources": ["rds", "snapshot"], "provenance": prov3,
    }
    mock_type3.assert_called_once_with(["1", "2"], datetime(2026, 8, 23, 14, 30))


def test_navigation_values_type4_dispatches_to_get_toll_values_as_batch():
    prov4 = [
        {"storage_source": "rds", "value_basis": "segment_value"},
        {"storage_source": "code", "value_basis": "implicit_zero"},
    ]
    with patch(
        "src.serving.nav_api.get_toll_values_with_tiers",
        return_value=([2.75, 0.0], prov4),
    ) as mock_toll:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1", "2"], "type": 4, "date": "2026-08-23", "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "value": [2.75, 0.0], "sources": ["rds", "hardcoded"], "provenance": prov4,
    }
    mock_toll.assert_called_once_with(["1", "2"])


def test_navigation_values_rejects_type5():
    response = client.post(
        "/api/navigation/values",
        json={"segment_ids": ["1"], "type": 5, "date": "2026-08-23", "time": "12:00"},
    )

    assert response.status_code == 422


def test_navigation_values_rejects_malformed_date():
    response = client.post(
        "/api/navigation/values",
        json={"segment_ids": ["1"], "type": 1, "date": "2026/08/23", "time": "12:00"},
    )

    assert response.status_code == 422


def test_navigation_values_rejects_too_many_segment_ids():
    response = client.post(
        "/api/navigation/values",
        json={
            "segment_ids": [str(i) for i in range(501)],
            "type": 1,
            "date": "2026-08-23",
            "time": "12:00",
        },
    )

    assert response.status_code == 422


def test_cors_preflight_request_succeeds():
    # API Gateway가 catch-all 라우트로 붙어있으면 OPTIONS가 API Gateway
    # 선에서 처리 안 되고 그대로 Lambda까지 넘어올 수 있다 - 이때도 FastAPI가
    # 직접 200 + CORS 헤더로 응답해야 브라우저가 preflight를 통과시킨다.
    response = client.options(
        "/api/navigation/values",
        headers={
            "Origin": "http://example.com",
            "Access-Control-Request-Method": "POST",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"


def test_cors_actual_response_includes_allow_origin_header():
    with patch(
        "src.serving.nav_api.resolve_segment_values_with_tiers",
        return_value=([30], [{"storage_source": "rds", "value_basis": "observed"}]),
    ):
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1"], "type": 1, "date": "2026-08-23", "time": "12:00"},
            headers={"Origin": "http://example.com"},
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"
