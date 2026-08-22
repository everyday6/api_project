from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.common.config import AWS_REGION
from src.serving import nav_lookup


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.DYNAMODB_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.DYNAMODB_TABLE_TYPE2


def _create_table(table_name, region=AWS_REGION):
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


@mock_aws
def test_resolve_skips_malformed_item_and_falls_through():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import get_table

    # "value" 필드가 없는 깨진 항목을 직접 DynamoDB에 넣음(put_item 헬퍼로는 못 만드니 저수준으로)
    get_table(nav_lookup.DYNAMODB_TABLE_TYPE1).put_item(Item={"segment_id": "1", "sk": "1200"})
    from src.common.dynamodb import put_item
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "AVG", "value": 40})

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_never_raises_on_invalid_type():
    result = nav_lookup.resolve_segment_values(["1", "2"], 3, "12:00")

    assert len(result) == 2
    assert all(isinstance(v, int) for v in result)


def test_resolve_never_raises_on_malformed_time():
    result = nav_lookup.resolve_segment_values(["1"], 1, "not-a-time")

    assert len(result) == 1
    assert isinstance(result[0], int)


def test_add_seconds_advances_within_same_hour():
    assert nav_lookup._add_seconds("12:00", 600) == "12:10"


def test_add_seconds_wraps_past_midnight():
    assert nav_lookup._add_seconds("23:50", 900) == "00:05"


@mock_aws
def test_resolve_time_values_uses_cumulative_elapsed_time_per_segment():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    # 세그먼트 1: 12:00 버킷에 1800초(30분) 소요.
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "1200", "value": 1800})
    # 세그먼트 2: 12:00 버킷과 12:30 버킷에 서로 다른 값 -> 누적 시각이
    # 제대로 반영되면 12:30 버킷 값(999)을 써야 한다.
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "2", "sk": "1200", "value": 111})
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "2", "sk": "1230", "value": 999})

    result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [1800, 999]


@mock_aws
def test_resolve_time_values_same_segment_twice_uses_different_buckets():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "loop", "sk": "1200", "value": 1800})
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "loop", "sk": "1230", "value": 77})

    # 같은 세그먼트가 경로에 두 번 등장 - 두 번째 등장은 첫 번째 소요시간만큼
    # 시각이 밀려 다른 버킷(1230)을 봐야 하므로 값도 달라야 한다.
    result = nav_lookup.resolve_segment_values(["loop", "loop"], 1, "12:00")

    assert result == [1800, 77]


@mock_aws
def test_resolve_type2_still_dedupes_since_time_independent():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "LENGTH", "value": 500})

    result = nav_lookup.resolve_segment_values(["1", "1", "1"], 2, "09:00")

    assert result == [500, 500, 500]
