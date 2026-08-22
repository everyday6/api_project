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
