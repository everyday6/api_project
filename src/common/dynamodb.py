"""
DynamoDB 공용 접근 헬퍼

boto3 저수준 배치 조회/저장만 감싼다. fallback 체인(정확 값 -> AVG ->
GLOBAL#DEFAULT -> 코드 상수) 같은 비즈니스 로직은 여기 두지 않는다 —
서빙 API(src/serving/nav_api.py)가 이 모듈의 batch_get_items()를 호출해서
"없는 키는 결과에 없다"는 사실 자체를 fallback 트리거로 쓴다.

배치 조회 시 키가 없는 경우와 DynamoDB 호출 자체가 실패(예외)하는 경우를
호출부가 구분해서 처리할 수 있도록, 이 모듈은 예외를 삼키지 않고 그대로
던진다 — 호출부(서빙 API)가 그 예외를 잡아서 fallback으로 넘어간다.
"""

from __future__ import annotations

import boto3

from src.common.config import AWS_REGION, DYNAMODB_ENDPOINT_URL
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="dynamodb")

# DynamoDB BatchGetItem 한 번에 요청 가능한 최대 키 개수(AWS 하드 리밋).
_BATCH_GET_MAX_KEYS = 100


def get_dynamodb_resource():
    """DynamoDB 리소스를 반환한다.

    APP_ENV=local이면 DYNAMODB_ENDPOINT_URL(dynamodb-local 컨테이너)을 쓰고,
    아니면 기본 AWS 엔드포인트를 쓴다.
    """
    kwargs = {"region_name": AWS_REGION}
    if DYNAMODB_ENDPOINT_URL:
        kwargs["endpoint_url"] = DYNAMODB_ENDPOINT_URL

    return boto3.resource("dynamodb", **kwargs)


def get_table(table_name: str):
    """테이블 핸들을 반환한다."""
    return get_dynamodb_resource().Table(table_name)


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
                result[(item["segment_id"], item["sk"])] = item

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
    get_table(table_name).put_item(Item=item)


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
            writer.put_item(Item=item)
