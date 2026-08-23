"""
DynamoDB 공용 접근 헬퍼

boto3 저수준 배치 조회/저장만 감싼다. fallback 체인(정확 값 -> AVG ->
GLOBAL#DEFAULT -> 코드 상수) 같은 비즈니스 로직은 여기 두지 않는다 —
서빙 조회(src/serving/nav_lookup.py)가 이 모듈의 batch_get_items()를 호출해서
"없는 키는 결과에 없다"는 사실 자체를 fallback 트리거로 쓴다.

get_value/put_item/batch_write_items는 float를 Decimal로 자동 변환해서
쓴다 — boto3가 raw float을 거부하기 때문(TypeError: Float types are not
supported). toll처럼 실제 소수값(0.75, 17.00 등)을 쓰는 호출부도 이 모듈
하나로 통일해서 쓸 수 있게 하기 위함.

배치 조회 시 키가 없는 경우와 DynamoDB 호출 자체가 실패(예외)하는 경우를
호출부가 구분해서 처리할 수 있도록, 이 모듈은 예외를 삼키지 않고 그대로
던진다 — 호출부(서빙 API)가 그 예외를 잡아서 fallback으로 넘어간다.
"""

from __future__ import annotations

from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from src.common.config import AWS_REGION
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="dynamodb")

# DynamoDB BatchGetItem 한 번에 요청 가능한 최대 키 개수(AWS 하드 리밋).
_BATCH_GET_MAX_KEYS = 100


def _floats_to_decimals(value):
    """DynamoDB(boto3)는 Python float을 못 받고 Decimal만 받는다
    (TypeError: Float types are not supported). str로 한 번 거쳐 변환해서
    이진부동소수점 오차가 Decimal로 그대로 옮겨붙는 걸 피한다.

    이 파이프라인들(nav_length/nav_time)은 관례상 value를 항상 round()해서
    int로 넘기므로 이 변환이 사실상 no-op이지만, toll처럼 실제 소수(0.75,
    17.00 등)를 쓰는 호출부에서 TypeError 없이 그대로 쓸 수 있게 공용
    모듈 레벨에서 처리한다."""

    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _floats_to_decimals(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_floats_to_decimals(v) for v in value]
    return value


def _decimals_to_floats(value):
    """조회 결과(Decimal)를 다시 보통 숫자로 되돌린다 — 소비처가 Decimal을
    몰라도 되게 하기 위함. 정수 값이면 int로, 아니면 float로 돌려준다."""

    if isinstance(value, Decimal):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, dict):
        return {k: _decimals_to_floats(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decimals_to_floats(v) for v in value]
    return value


def get_dynamodb_resource():
    """DynamoDB 리소스를 반환한다.

    기본 재시도 설정(legacy)은 스로틀링(RequestLimitExceeded 등)을 만나면
    금방 포기한다. Type 3 롤링 값 적재처럼 executor 여러 개가 동시에
    BatchWriteItem을 몰아치는 경우 순간적으로 계정/테이블 처리량 한도를
    넘기기 쉬운데, adaptive 모드로 재시도 횟수를 늘려 자동 백오프 후
    재시도하게 한다."""
    config = Config(retries={"mode": "adaptive", "max_attempts": 15})
    return boto3.resource("dynamodb", region_name=AWS_REGION, config=config)


def get_table(table_name: str):
    """테이블 핸들을 반환한다."""
    return get_dynamodb_resource().Table(table_name)


def ensure_table(table_name: str) -> None:
    """테이블이 없으면 만든다. 로컬 개발/테스트 편의용이다 — 실 AWS
    테이블은 배포 시점에 scripts/create_dynamodb_tables.py로 미리 만들어두고,
    운영 파이프라인 코드 경로에서 매번 이걸 부르는 건 피한다(실수로 스키마를
    바꾸는 걸 막기 위함)."""

    client = get_dynamodb_resource().meta.client
    if table_name in client.list_tables()["TableNames"]:
        return

    table = get_dynamodb_resource().create_table(
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


def _chunk(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def batch_get_items(table_name: str, keys: list[dict]) -> dict[tuple[str, str], dict]:
    """(segment_id, sk) 키 목록으로 여러 항목을 한 번에 조회한다.

    100개 초과분은 여러 BatchGetItem 요청으로 자동 청크 분할한다. 반환값은
    (segment_id, sk) -> item 딕셔너리이며, DynamoDB에 없는 키는 결과에서
    빠진다(호출부가 이걸로 fallback 여부를 판단한다).
    """
    if not keys:
        return {}

    resource = get_dynamodb_resource()
    result: dict[tuple[str, str], dict] = {}

    for chunk in _chunk(keys, _BATCH_GET_MAX_KEYS):
        request_keys = list(chunk)

        # DynamoDB가 처리량 제한 등으로 일부만 처리하고 나머지를
        # UnprocessedKeys로 돌려줄 수 있다 — 전부 처리될 때까지 재요청한다.
        while request_keys:
            response = resource.batch_get_item(
                RequestItems={table_name: {"Keys": request_keys}}
            )

            for item in response["Responses"].get(table_name, []):
                result[(item["segment_id"], item["sk"])] = _decimals_to_floats(item)

            unprocessed = response.get("UnprocessedKeys", {})
            request_keys = unprocessed.get(table_name, {}).get("Keys", [])

            if request_keys:
                logger.warning(
                    "DynamoDB batch_get_item 미처리 키 재요청: table=%s count=%d",
                    table_name,
                    len(request_keys),
                )

    return result


def put_item(table_name: str, item: dict) -> None:
    """항목 하나를 저장(upsert)한다."""
    get_table(table_name).put_item(Item=_floats_to_decimals(item))


def batch_write_items(table_name: str, items: list[dict]) -> None:
    """여러 항목을 한 번에 저장(upsert)한다.

    파이프라인 Gold2 단계가 세그먼트 수천~수십만 건을 한 번에 upsert할 때
    쓴다. boto3 Table.batch_writer()가 내부적으로 25개 단위 BatchWriteItem
    요청과 처리량 제한 시 자동 재시도까지 처리한다.
    """
    if not items:
        return

    table = get_table(table_name)
    with table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=_floats_to_decimals(item))


def get_value(table_name: str, segment_id: str, sk: str, default=0):
    """(segment_id, sk) 하나를 조회한다. 없으면 default를 반환한다 —
    "무결점 응답" 원칙: 값이 없어도, 심지어 테이블 자체가 아직 안
    만들어졌어도(Gold 파이프라인이 한 번도 안 돈 경우 등) 절대 None/에러를
    반환하지 않는다."""

    try:
        response = get_table(table_name).get_item(Key={"segment_id": segment_id, "sk": sk})
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            return default
        raise
    item = response.get("Item")
    return _decimals_to_floats(item["value"]) if item is not None else default
