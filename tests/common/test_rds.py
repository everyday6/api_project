import time

import psycopg2
import pytest

from src.common import rds
from src.common.config import NAV_GOLD_RDS_LOCAL_DSN

TABLE_NAME = "test_segment_metrics_type1_rds"


@pytest.fixture
def rds_table(monkeypatch):
    """실제 로컬 Postgres(docker-compose의 nav-gold-postgres 컨테이너,
    미리 `docker compose up -d nav-gold-postgres`로 띄워둬야 한다)로
    검증한다 - RDS는 DynamoDB의 moto 같은 인메모리 mock 수단이 없다."""
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: NAV_GOLD_RDS_LOCAL_DSN)
    rds._connection = None
    rds.ensure_table(TABLE_NAME)
    conn = rds.get_connection()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    yield TABLE_NAME
    rds._connection = None


def test_ensure_table_is_idempotent(rds_table):
    # 두 번째 호출은 이미 있으니 그냥 조용히 넘어가야 한다(예외 없음).
    rds.ensure_table(rds_table)


def test_upsert_then_resolve_type1_tiers_returns_all_three_tiers(rds_table):
    now = time.time()
    rds.upsert_items([
        {"segment_id": "1", "sk": "1200", "value": 30.0, "observed_at": now, "collected_date": None, "count": None},
        {"segment_id": "1", "sk": "AVG", "value": 40.0, "observed_at": None, "collected_date": None, "count": 5},
        {"segment_id": "1", "sk": "SPEC", "value": 50.0, "observed_at": None, "collected_date": None, "count": None},
    ], rds_table)

    tiers = rds.resolve_type1_tiers("1", "1200", rds_table)

    assert tiers["1200"]["value"] == 30.0
    assert tiers["1200"]["observed_at"] == pytest.approx(now)
    assert tiers["AVG"]["value"] == 40.0
    assert tiers["SPEC"]["value"] == 50.0


def test_resolve_type1_tiers_omits_missing_tiers(rds_table):
    rds.upsert_items([
        {"segment_id": "1", "sk": "AVG", "value": 40.0, "observed_at": None, "collected_date": None, "count": 5},
    ], rds_table)

    tiers = rds.resolve_type1_tiers("1", "1200", rds_table)

    assert "1200" not in tiers
    assert "SPEC" not in tiers
    assert tiers["AVG"]["value"] == 40.0


def test_resolve_type1_tiers_returns_empty_dict_for_unknown_segment(rds_table):
    tiers = rds.resolve_type1_tiers("no-such-segment", "1200", rds_table)

    assert tiers == {}


def test_upsert_items_updates_existing_row_on_conflict(rds_table):
    rds.upsert_items([
        {"segment_id": "1", "sk": "AVG", "value": 10.0, "observed_at": None, "collected_date": None, "count": 1},
    ], rds_table)
    rds.upsert_items([
        {"segment_id": "1", "sk": "AVG", "value": 20.0, "observed_at": None, "collected_date": None, "count": 2},
    ], rds_table)

    tiers = rds.resolve_type1_tiers("1", "AVG", rds_table)

    assert tiers["AVG"]["value"] == 20.0


def test_upsert_items_empty_list_returns_zero(rds_table):
    assert rds.upsert_items([], rds_table) == 0


def test_batch_get_rows_returns_only_matching_keys(rds_table):
    rds.upsert_items([
        {"segment_id": "1", "sk": "AVG", "value": 40.0, "observed_at": None, "collected_date": None, "count": 5},
        {"segment_id": "2", "sk": "SPEC", "value": 60.0, "observed_at": None, "collected_date": None, "count": None},
    ], rds_table)

    result = rds.batch_get_rows(rds_table, [("1", "AVG"), ("2", "SPEC"), ("999", "AVG")])

    assert result[("1", "AVG")]["value"] == 40.0
    assert result[("2", "SPEC")]["value"] == 60.0
    assert ("999", "AVG") not in result


def test_batch_get_rows_empty_keys_returns_empty_dict(rds_table):
    assert rds.batch_get_rows(rds_table, []) == {}


def test_export_snapshot_source_merges_avg_spec_and_latest_exact(rds_table):
    now = time.time()
    rds.upsert_items([
        {"segment_id": "1", "sk": "1200", "value": 20.0, "observed_at": now - 3600, "collected_date": None, "count": None},
        {"segment_id": "1", "sk": "1230", "value": 25.0, "observed_at": now, "collected_date": None, "count": None},
        {"segment_id": "1", "sk": "AVG", "value": 22.0, "observed_at": None, "collected_date": None, "count": 2},
        {"segment_id": "1", "sk": "SPEC", "value": 30.0, "observed_at": None, "collected_date": None, "count": None},
    ], rds_table)

    snapshot = rds.export_snapshot_source(rds_table)

    assert snapshot["1"]["avg"] == 22.0
    assert snapshot["1"]["spec"] == 30.0
    # 가장 최근(observed_at 기준) exact 값 하나만 포함되어야 한다.
    assert snapshot["1"]["exact_value"] == 25.0
    assert snapshot["1"]["exact_observed_at"] == pytest.approx(now)


def test_export_snapshot_source_excludes_exact_rows_without_observed_at(rds_table):
    # observed_at이 없는 exact 행(레거시/이상 데이터)은 신선도 판단이
    # 불가능하니 스냅샷의 exact 후보에서 제외되어야 한다.
    rds.upsert_items([
        {"segment_id": "1", "sk": "1200", "value": 20.0, "observed_at": None, "collected_date": None, "count": None},
    ], rds_table)

    snapshot = rds.export_snapshot_source(rds_table)

    assert "exact_value" not in snapshot.get("1", {})


def test_connection_failure_propagates_as_operational_error(monkeypatch):
    # 연결 자체가 실패하면 예외를 삼키지 않고 그대로 던져야 한다 -
    # 호출부(nav_lookup)가 이걸로 "RDS 자체가 죽었다"를 판단해서
    # memory/S3 폴백으로 넘어간다.
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: "postgresql://nav_gold:nav_gold@localhost:1/nav_gold")
    rds._connection = None

    with pytest.raises(psycopg2.OperationalError):
        rds.resolve_type1_tiers("1", "1200", "irrelevant_table")

    rds._connection = None
