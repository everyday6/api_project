from datetime import datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from src.serving import api


class FakeDynamoDB:
    def __init__(self, values=None, fail=False):
        self.values = values or {}
        self.fail = fail
        self.calls = []

    def batch_get_item(self, RequestItems):
        if self.fail:
            raise RuntimeError("temporary DynamoDB failure")
        self.calls.append(RequestItems)
        table_name, request = next(iter(RequestItems.items()))
        items = [
            {
                "segment_id": key["segment_id"],
                "sk": key["sk"],
                "value": Decimal(str(self.values[key["segment_id"]])),
            }
            for key in reversed(request["Keys"])
            if key["segment_id"] in self.values
        ]
        return {"Responses": {table_name: items}, "UnprocessedKeys": {}}


@pytest.fixture(autouse=True)
def clear_value_cache():
    with api._cache_lock:
        api._value_cache.clear()
    api.get_dynamodb_resource.cache_clear()
    yield
    api.get_dynamodb_resource.cache_clear()


def test_build_sort_key_floors_to_30_minute_slot():
    assert api.build_sort_key(3, datetime(2026, 8, 21, 12, 29, 59)) == "3#FRI#1200"
    assert api.build_sort_key(3, datetime(2026, 8, 24, 12, 30, 0)) == "3#MON#1230"


def test_dynamodb_resource_uses_short_operational_timeouts(monkeypatch):
    calls = []
    expected_resource = object()

    def fake_resource(service_name, **kwargs):
        calls.append((service_name, kwargs))
        return expected_resource

    monkeypatch.setattr(api, "DYNAMODB_NAV_TABLE", "navigation-values")
    monkeypatch.setattr(api.boto3, "resource", fake_resource)

    assert api.get_dynamodb_resource() is expected_resource
    assert api.get_dynamodb_resource() is expected_resource
    assert len(calls) == 1

    service_name, kwargs = calls[0]
    config = kwargs["config"]
    assert service_name == "dynamodb"
    assert config.connect_timeout == 1
    assert config.read_timeout == 1
    assert config.retries["total_max_attempts"] == 2
    assert config.max_pool_connections == 50


def test_navigation_request_is_validated_and_normalized_by_pydantic():
    request = api.NavigationValuesRequest.model_validate(
        [[" 0077356 "], 3, "2026-08-21T12:00:00"]
    )

    assert request.root == (["0077356"], 3, datetime(2026, 8, 21, 12, 0))


def test_get_type3_values_batches_and_preserves_input_order():
    segment_ids = [f"{value:07d}" for value in range(101)]
    requested = [segment_ids[100], segment_ids[0], segment_ids[100], *segment_ids[1:100]]
    dynamodb = FakeDynamoDB({segment_id: index for index, segment_id in enumerate(segment_ids)})

    result = api.get_type3_values(
        requested,
        datetime(2026, 8, 21, 12, 0),
        dynamodb=dynamodb,
        table_name="navigation-values",
    )

    assert result == [100.0, 0.0, 100.0, *[float(value) for value in range(1, 100)]]
    assert len(dynamodb.calls) == 2
    assert max(
        len(next(iter(call.values()))["Keys"])
        for call in dynamodb.calls
    ) == 100


def test_get_type3_values_uses_cache_then_zero_on_dynamodb_failure():
    requested_at = datetime(2026, 8, 21, 12, 0)
    api.get_type3_values(
        ["0077356"],
        requested_at,
        dynamodb=FakeDynamoDB({"0077356": 18.5}),
        table_name="navigation-values",
    )

    result = api.get_type3_values(
        ["0077356", "0088421"],
        requested_at,
        dynamodb=FakeDynamoDB(fail=True),
        table_name="navigation-values",
    )

    assert result == [18.5, 0.0]


def test_navigation_values_accepts_agreed_array_request(monkeypatch):
    monkeypatch.setattr(
        api,
        "get_type3_values",
        lambda segment_ids, requested_at: [18.5, 7.0],
    )
    client = TestClient(api.app)

    response = client.post(
        "/api/navigation/values",
        json=[["0077356", "0088421"], 3, "2026-08-21T12:00:00"],
    )

    assert response.status_code == 200
    assert response.json() == [18.5, 7.0]


def test_navigation_values_rejects_unsupported_type():
    client = TestClient(api.app)

    response = client.post(
        "/api/navigation/values",
        json=[["0077356"], 4, "2026-08-21T12:00:00"],
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "literal_error"
