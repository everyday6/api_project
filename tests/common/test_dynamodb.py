import boto3
import pytest
from moto import mock_aws

from src.common import dynamodb
from src.common.config import AWS_REGION


TABLE_NAME = "TestSegmentMetrics"


def _create_test_table(region=AWS_REGION):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "segment_id", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "segment_id", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@mock_aws
def test_put_item_then_batch_get_returns_it():
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "1", "sk": "1200", "value": 30})

    result = dynamodb.batch_get_items(
        TABLE_NAME, [{"segment_id": "1", "sk": "1200"}]
    )

    assert result[("1", "1200")]["value"] == 30


@mock_aws
def test_batch_get_missing_key_is_absent_from_result():
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "1", "sk": "1200", "value": 30})

    result = dynamodb.batch_get_items(
        TABLE_NAME,
        [{"segment_id": "1", "sk": "1200"}, {"segment_id": "999", "sk": "1200"}],
    )

    assert ("1", "1200") in result
    assert ("999", "1200") not in result


@mock_aws
def test_batch_get_chunks_over_100_keys():
    _create_test_table()

    for i in range(150):
        dynamodb.put_item(TABLE_NAME, {"segment_id": str(i), "sk": "1200", "value": i})

    keys = [{"segment_id": str(i), "sk": "1200"} for i in range(150)]
    result = dynamodb.batch_get_items(TABLE_NAME, keys)

    assert len(result) == 150
    assert result[("149", "1200")]["value"] == 149


@mock_aws
def test_batch_get_empty_keys_returns_empty_dict():
    _create_test_table()

    result = dynamodb.batch_get_items(TABLE_NAME, [])

    assert result == {}


@mock_aws
def test_batch_write_items_then_get_all():
    _create_test_table()

    items = [{"segment_id": str(i), "sk": "LENGTH", "value": i * 10} for i in range(30)]
    dynamodb.batch_write_items(TABLE_NAME, items)

    result = dynamodb.batch_get_items(
        TABLE_NAME, [{"segment_id": str(i), "sk": "LENGTH"} for i in range(30)]
    )

    assert len(result) == 30
    assert result[("29", "LENGTH")]["value"] == 290


@mock_aws
def test_put_item_and_batch_get_items_round_trip_floats():
    # toll처럼 실제 소수값을 쓰는 호출부가 TypeError 없이 그대로 쓸 수
    # 있어야 한다(Decimal 변환은 이 모듈 안에서 처리).
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "S1", "sk": "TYPE#4", "value": 0.75})

    result = dynamodb.batch_get_items(TABLE_NAME, [{"segment_id": "S1", "sk": "TYPE#4"}])

    assert result[("S1", "TYPE#4")]["value"] == 0.75


@mock_aws
def test_get_value_returns_written_value():
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "S1", "sk": "TYPE#4", "value": 0.75})

    result = dynamodb.get_value(TABLE_NAME, "S1", "TYPE#4")

    assert result == 0.75


@mock_aws
def test_get_value_returns_default_when_missing():
    _create_test_table()

    result = dynamodb.get_value(TABLE_NAME, "NO_SUCH_SEGMENT", "TYPE#4", default=0)

    assert result == 0


def test_get_value_returns_default_when_table_does_not_exist():
    # ensure_table을 아예 안 부른 테이블 — Gold 파이프라인이 한 번도 안
    # 돈 상태를 재현한다. 에러 없이 default가 나와야 한다("무결점 응답").
    with mock_aws():
        result = dynamodb.get_value("table_that_does_not_exist", "S1", "TYPE#4", default=0)

    assert result == 0


def test_ensure_table_creates_table_once_and_is_idempotent():
    with mock_aws():
        dynamodb.ensure_table(TABLE_NAME)
        # 두 번째 호출은 이미 있으니 그냥 조용히 넘어가야 한다(예외 없음).
        dynamodb.ensure_table(TABLE_NAME)

        client = dynamodb.get_dynamodb_resource().meta.client
        assert TABLE_NAME in client.list_tables()["TableNames"]
