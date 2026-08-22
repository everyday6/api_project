import pytest

from src.common import dynamo

TEST_TABLE = "test_nav_gold_values"


@pytest.fixture(autouse=True)
def _clean_table():
    dynamo.ensure_table(TEST_TABLE)
    table = dynamo.get_resource().Table(TEST_TABLE)
    yield
    # 각 테스트 끝나고 넣은 아이템 지우기 (테이블 자체는 재사용)
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan["Items"]:
            batch.delete_item(Key={"segment_id": item["segment_id"], "sk": item["sk"]})


def test_put_item_and_get_value():
    dynamo.put_item({"segment_id": "S1", "sk": "TYPE#4", "value": 0.75}, table_name=TEST_TABLE)

    result = dynamo.get_value("S1", "TYPE#4", table_name=TEST_TABLE)

    assert result == 0.75


def test_get_value_returns_default_when_missing():
    result = dynamo.get_value("NO_SUCH_SEGMENT", "TYPE#4", table_name=TEST_TABLE, default=0)

    assert result == 0


def test_batch_write_and_batch_get_values_preserves_order():
    dynamo.batch_write_items(
        [
            {"segment_id": "S1", "sk": "TYPE#5", "value": 6.94},
            {"segment_id": "S2", "sk": "TYPE#5", "value": 10.67},
        ],
        table_name=TEST_TABLE,
    )

    result = dynamo.batch_get_values(["S2", "S1", "S_MISSING"], "TYPE#5", table_name=TEST_TABLE, default=0)

    assert result == [10.67, 6.94, 0]


def test_batch_get_values_handles_more_than_100_segments():
    items = [{"segment_id": f"S{i}", "sk": "TYPE#4", "value": 0.75} for i in range(120)]
    dynamo.batch_write_items(items, table_name=TEST_TABLE)

    segment_ids = [f"S{i}" for i in range(120)]
    result = dynamo.batch_get_values(segment_ids, "TYPE#4", table_name=TEST_TABLE, default=0)

    assert result == [0.75] * 120
