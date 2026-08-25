from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from src.serving import api


class FakeCursor:
    def __init__(self, parent):
        self._parent = parent
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        if self._parent.fail:
            raise RuntimeError("temporary RDS failure")
        if params is None:
            # 헬스체크 쿼리("SELECT 1") — 정상 연결로 취급한다.
            self._result = []
            return
        # flat 스키마(segment_id, dow, time) 조회라 dow/time도 함께 넘어온다
        # (src/serving/api.py의 _fetch_batch 참고) — fake는 실제 dow/time
        # 매칭 없이 항상 같은 값을 돌려줘서, 테스트가 어떤 requested_at을
        # 쓰든 값을 찾을 수 있게 한다.
        segment_ids, _dow, _bucket = params
        self._parent.calls.append(list(segment_ids))
        self._result = [
            (segment_id, self._parent.values[segment_id])
            for segment_id in reversed(segment_ids)
            if segment_id in self._parent.values
        ]

    def fetchall(self):
        return self._result


class FakeConnection:
    """RDS 커넥션을 흉내내는 fake — get_type3_values()에 conn=으로 직접
    주입해서 실제 DB 없이 조회 로직만 검증한다."""

    closed = False

    def __init__(self, values=None, fail=False):
        self.values = values or {}
        self.fail = fail
        self.calls = []

    def cursor(self):
        return FakeCursor(self)


@pytest.fixture(autouse=True)
def clear_value_cache():
    with api._cache_lock:
        api._value_cache.clear()
    api._db_connection = None
    yield
    api._db_connection = None


def test_build_sort_key_floors_to_30_minute_slot():
    assert api.build_sort_key(3, datetime(2026, 8, 21, 12, 29, 59)) == "3#FRI#1200"
    assert api.build_sort_key(3, datetime(2026, 8, 24, 12, 30, 0)) == "3#MON#1230"


def test_db_connection_uses_short_operational_timeouts(monkeypatch):
    calls = []

    class FakeConn:
        closed = False
        autocommit = False

        def cursor(self):
            return FakeCursor(FakeConnection())

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return FakeConn()

    monkeypatch.setattr(api, "SERVING_TABLE_TYPE3", "navigation-values")
    monkeypatch.setattr(api, "RDS_HOST", "localhost")
    monkeypatch.setattr(api, "RDS_DB", "navdb")
    monkeypatch.setattr(api.psycopg2, "connect", fake_connect)

    assert api.get_db_connection() is api.get_db_connection()
    assert len(calls) == 1

    kwargs = calls[0]
    assert kwargs["connect_timeout"] == 1
    assert "statement_timeout=1000" in kwargs["options"]


def test_navigation_request_is_validated_and_normalized_by_pydantic():
    request = api.NavigationValuesRequest.model_validate(
        [[" 0077356 "], 3, "2026-08-21T12:00:00"]
    )

    assert request.root == (["0077356"], 3, datetime(2026, 8, 21, 12, 0))


def test_get_type3_values_batches_and_preserves_input_order():
    segment_ids = [f"{value:07d}" for value in range(101)]
    requested = [segment_ids[100], segment_ids[0], segment_ids[100], *segment_ids[1:100]]
    conn = FakeConnection({segment_id: index for index, segment_id in enumerate(segment_ids)})

    result = api.get_type3_values(
        requested,
        datetime(2026, 8, 21, 12, 0),
        conn=conn,
        table_name="navigation-values",
    )

    assert result == [100.0, 0.0, 100.0, *[float(value) for value in range(1, 100)]]
    assert len(conn.calls) == 2
    assert max(len(call) for call in conn.calls) == 100


def test_get_type3_values_logs_rds_query_duration(caplog):
    # db.py의 batch_get_items()와 같은 형식의 로그 - Grafana의 "타입별 RDS
    # 쿼리 응답시간" 패널이 table 필드로 두 경로(db.py/api.py)를 같이
    # 집계한다.
    conn = FakeConnection({"0077356": 18.5})

    with caplog.at_level("INFO", logger="src.serving.api"):
        api.get_type3_values(
            ["0077356"],
            datetime(2026, 8, 21, 12, 0),
            conn=conn,
            table_name="navigation-values",
        )

    duration_logs = [r.message for r in caplog.records if "[rds_query_duration]" in r.message]
    assert len(duration_logs) == 1
    assert "table=navigation-values" in duration_logs[0]


def test_get_type3_values_uses_cache_then_zero_on_rds_failure():
    requested_at = datetime(2026, 8, 21, 12, 0)
    api.get_type3_values(
        ["0077356"],
        requested_at,
        conn=FakeConnection({"0077356": 18.5}),
        table_name="navigation-values",
    )

    result = api.get_type3_values(
        ["0077356", "0088421"],
        requested_at,
        conn=FakeConnection(fail=True),
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
