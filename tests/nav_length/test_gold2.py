import pytest
from pyspark.sql import SparkSession

from src.common import rds
from src.common.config import NAV_GOLD_RDS_LOCAL_DSN
from src.nav_length import gold2

TABLE_NAME = "test_segment_metrics_type2"


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold2_test").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def rds_table(monkeypatch):
    """실제 로컬 Postgres(docker-compose의 nav-gold-postgres 컨테이너,
    미리 `docker compose up -d nav-gold-postgres`로 띄워둬야 한다)로
    검증한다 - RDS는 DynamoDB의 moto 같은 인메모리 mock 수단이 없다."""
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: NAV_GOLD_RDS_LOCAL_DSN)
    rds._connection = None
    rds.ensure_static_table(TABLE_NAME)
    conn = rds.get_connection()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    yield TABLE_NAME
    rds._connection = None


def test_to_type2_items_rounds_length_to_int(spark):
    df = spark.createDataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_type2_items(df)

    assert len(items) == 1
    assert items[0]["segment_id"] == "1"
    assert items[0]["value"] == 121


def test_to_type2_items_includes_collected_and_updated_date(spark):
    from datetime import date

    df = spark.createDataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_type2_items(df)

    today = date.today().isoformat()
    assert items[0]["collected_date"] == today
    assert items[0]["updated_date"] == today


def test_to_type2_items_multiple_rows(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
    ])

    items = gold2.to_type2_items(df)

    assert len(items) == 2
    assert {item["segment_id"]: item["value"] for item in items} == {"1": 100, "2": 200}


def test_write_to_rds_upserts_and_returns_count(rds_table):
    items = [{"segment_id": "1", "value": 100, "collected_date": "2026-08-24", "updated_date": "2026-08-24"}]

    count = gold2.write_to_rds(items, rds_table)

    assert count == 1
    result = rds.batch_get_static_values(rds_table, ["1"])
    assert result["1"]["value"] == 100.0


def test_write_to_rds_updates_existing_row_on_conflict(rds_table):
    gold2.write_to_rds([{"segment_id": "1", "value": 100}], rds_table)
    gold2.write_to_rds([{"segment_id": "1", "value": 150}], rds_table)

    result = rds.batch_get_static_values(rds_table, ["1"])
    assert result["1"]["value"] == 150.0
