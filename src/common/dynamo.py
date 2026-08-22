"""
DynamoDB 서빙 모듈 — nav 골드 데이터셋(segment_id x type 조회) 전용

src/common/db.py(RDS)와 같은 위치의 서빙 레이어지만, nav 골드 데이터셋의
접근 패턴(segment_id 목록 x type 하나로 값 목록 조회)이 순수 key-value라
DynamoDB를 쓴다. 테이블 하나(NAV_GOLD_TABLE)에 모든 타입을 담고, sort key
접두사(예: "TYPE#4")로 타입을 구분한다 — 타입마다 테이블을 나누면 RDS의
write_table() 전체 replace 같은 문제(한 타입 갱신이 다른 타입을 덮어씀)가
DynamoDB에선 애초에 없다(아이템 단위 쓰기라서). 자세한 배경은
docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md 참고.
"""

from __future__ import annotations

from decimal import Decimal

import boto3

from src.common.config import APP_ENV, DYNAMO_LOCAL_ENDPOINT, DYNAMO_REGION, NAV_GOLD_TABLE

_resource = None


def _floats_to_decimals(value):
    """DynamoDB(boto3)는 Python float을 못 받고 Decimal만 받는다
    (TypeError: Float types are not supported). str로 한 번 거쳐 변환해서
    이진부동소수점 오차가 Decimal로 그대로 옮겨붙는 걸 피한다."""

    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _floats_to_decimals(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_floats_to_decimals(v) for v in value]
    return value


def _decimals_to_floats(value):
    """조회 결과(Decimal)를 다시 보통 숫자로 되돌린다 — 소비처(API
    응답 등)가 Decimal을 몰라도 되게 하기 위함."""

    if isinstance(value, Decimal):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, dict):
        return {k: _decimals_to_floats(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decimals_to_floats(v) for v in value]
    return value


def get_resource():
    """DynamoDB 리소스를 반환한다(프로세스당 한 번만 생성해서 재사용).
    APP_ENV=local이면 dynamodb-local에, 아니면 실 DynamoDB에 붙는다."""

    global _resource

    if _resource is None:
        if APP_ENV == "local":
            _resource = boto3.resource(
                "dynamodb",
                region_name=DYNAMO_REGION,
                endpoint_url=DYNAMO_LOCAL_ENDPOINT,
                aws_access_key_id="local",
                aws_secret_access_key="local",
            )
        else:
            _resource = boto3.resource("dynamodb", region_name=DYNAMO_REGION)

    return _resource


def ensure_table(table_name: str = NAV_GOLD_TABLE) -> None:
    """테이블이 없으면 만든다. 로컬 개발/테스트 편의용이다 — 실 AWS
    테이블은 미리 만들어두고 운영 중에는 이 함수를 안 쓴다(실수로 스키마를
    바꾸는 걸 막기 위함)."""

    client = get_resource().meta.client
    if table_name in client.list_tables()["TableNames"]:
        return

    table = get_resource().create_table(
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
    table.wait_until_exists()


def put_item(item: dict, table_name: str = NAV_GOLD_TABLE) -> None:
    """아이템 하나를 쓴다. item은 최소 {segment_id, sk, value}를 포함해야 한다."""

    get_resource().Table(table_name).put_item(Item=_floats_to_decimals(item))


def batch_write_items(items: list[dict], table_name: str = NAV_GOLD_TABLE) -> None:
    """여러 아이템을 배치로 쓴다. boto3의 batch_writer가 25개 단위로
    알아서 나눠 보내고 실패 시 재시도한다."""

    table = get_resource().Table(table_name)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=_floats_to_decimals(item))


def get_value(segment_id: str, sk: str, table_name: str = NAV_GOLD_TABLE, default=0):
    """(segment_id, sk) 하나를 조회한다. 없으면 default를 반환한다 —
    nav 골드 데이터셋의 "무결점 응답" 원칙: 값이 없어도 절대 None/에러를
    반환하지 않는다."""

    response = get_resource().Table(table_name).get_item(Key={"segment_id": segment_id, "sk": sk})
    item = response.get("Item")
    return _decimals_to_floats(item["value"]) if item is not None else default


def batch_get_values(
    segment_ids: list[str],
    sk: str,
    table_name: str = NAV_GOLD_TABLE,
    default=0,
) -> list:
    """segment_id 목록 + 고정 sk로 값 목록을 조회한다. 응답 순서는
    요청한 segment_ids 순서와 항상 동일하다(DynamoDB BatchGetItem 자체는
    순서를 보장 안 해서 직접 맞춰준다). 없는 segment_id는 default로 채운다.
    """

    if not segment_ids:
        return []

    table = get_resource()
    found: dict[str, object] = {}

    # BatchGetItem은 한 번에 최대 100개 키만 허용한다.
    for i in range(0, len(segment_ids), 100):
        chunk = segment_ids[i : i + 100]
        keys = [{"segment_id": sid, "sk": sk} for sid in chunk]
        response = table.meta.client.batch_get_item(
            RequestItems={table_name: {"Keys": keys}}
        )
        for item in response["Responses"][table_name]:
            found[item["segment_id"]] = _decimals_to_floats(item["value"])

    return [found.get(sid, default) for sid in segment_ids]
