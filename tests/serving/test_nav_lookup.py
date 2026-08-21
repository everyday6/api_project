from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.serving import nav_lookup


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.DYNAMODB_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.DYNAMODB_TABLE_TYPE2


def _create_table(table_name, region="us-east-1"):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
        TableName=table_name,
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


@mock_aws
def test_resolve_uses_exact_bucket_value_when_present():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "1200", "value": 30})

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]


@mock_aws
def test_resolve_falls_back_to_avg_when_bucket_missing_type1():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "AVG", "value": 40})

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


@mock_aws
def test_resolve_falls_back_to_global_default_when_nothing_for_segment():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE1,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 45},
    )

    result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [45]


@mock_aws
def test_resolve_type2_has_no_avg_tier_goes_straight_to_default():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE2,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 300},
    )
    # type2는 sk가 항상 "LENGTH"라, "AVG" 항목이 있어도 안 쓰여야 한다.
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "AVG", "value": 999})

    result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [300]


def test_resolve_falls_back_to_hardcoded_constant_when_dynamodb_unreachable():
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 2


@mock_aws
def test_resolve_preserves_order_and_duplicates():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "LENGTH", "value": 100})
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "2", "sk": "LENGTH", "value": 200})

    result = nav_lookup.resolve_segment_values(["2", "1", "2"], 2, "12:00")

    assert result == [200, 100, 200]
