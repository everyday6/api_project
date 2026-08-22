"""내비게이션이 segment별 비용 값을 조회하는 FastAPI."""

from __future__ import annotations

import time
from collections import OrderedDict
from datetime import datetime
from functools import lru_cache
from threading import Lock
from typing import Annotated, Literal

import boto3
from botocore.config import Config
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import Field, RootModel, field_validator

from src.common.config import (
    AWS_REGION,
    DYNAMODB_NAV_TABLE,
    TLC_TYPE3_DOW_NAMES,
    TLC_TYPE3_ID,
)
from src.common.logger import get_logger


logger = get_logger(__name__, log_to_file=True, log_file_stem="navigation_api")

app = FastAPI(title="Navigation Segment Value API", version="1.0.0")

TYPE3_ID = TLC_TYPE3_ID
WEEKDAY_NAMES = TLC_TYPE3_DOW_NAMES
DYNAMODB_BATCH_SIZE = 100
MAX_SEGMENTS_PER_REQUEST = 1_000
MAX_BATCH_GET_ATTEMPTS = 3
VALUE_CACHE_SIZE = 50_000
MISSING_VALUE = 0.0
DYNAMODB_CONNECT_TIMEOUT_SECONDS = 1
DYNAMODB_READ_TIMEOUT_SECONDS = 1
DYNAMODB_SDK_TOTAL_ATTEMPTS = 2
DYNAMODB_MAX_POOL_CONNECTIONS = 50

DYNAMODB_CLIENT_CONFIG = Config(
    connect_timeout=DYNAMODB_CONNECT_TIMEOUT_SECONDS,
    read_timeout=DYNAMODB_READ_TIMEOUT_SECONDS,
    retries={
        "total_max_attempts": DYNAMODB_SDK_TOTAL_ATTEMPTS,
        "mode": "standard",
    },
    max_pool_connections=DYNAMODB_MAX_POOL_CONNECTIONS,
    tcp_keepalive=True,
)

_value_cache: OrderedDict[tuple[str, str], float] = OrderedDict()
_cache_lock = Lock()

SegmentId = Annotated[str, Field(min_length=1)]
SegmentIds = Annotated[
    list[SegmentId],
    Field(min_length=1, max_length=MAX_SEGMENTS_PER_REQUEST),
]


class NavigationValuesRequest(
    RootModel[tuple[SegmentIds, Literal[TYPE3_ID], datetime]]
):
    """``[[segment_id...], 3, 날짜시간]`` 요청 모델."""

    @field_validator("root")
    @classmethod
    def normalize_segment_ids(cls, value):
        segment_ids, type_id, requested_at = value
        normalized = [segment_id.strip() for segment_id in segment_ids]
        if any(not segment_id for segment_id in normalized):
            raise ValueError("segment_id는 빈 값이 아닌 문자열이어야 합니다")
        return normalized, type_id, requested_at


@app.exception_handler(Exception)
async def log_unexpected_exception(request: Request, exc: Exception):
    logger.error(
        "처리되지 않은 API 예외: %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


def build_sort_key(type_id: int, requested_at: datetime) -> str:
    """요청 날짜의 요일과 시각을 Type 3 반복 버킷 키로 변환한다."""

    if type_id != TYPE3_ID:
        raise ValueError(f"지원하지 않는 type입니다: {type_id}")
    slot_minute = (requested_at.minute // 30) * 30
    dow = WEEKDAY_NAMES[requested_at.weekday()]
    return f"{type_id}#{dow}#{requested_at.hour:02d}{slot_minute:02d}"


@lru_cache(maxsize=1)
def get_dynamodb_resource():
    if not DYNAMODB_NAV_TABLE:
        raise RuntimeError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")
    return boto3.resource(
        "dynamodb",
        region_name=AWS_REGION,
        config=DYNAMODB_CLIENT_CONFIG,
    )


def _remember_value(segment_id: str, sk: str, value: float) -> None:
    key = (segment_id, sk)
    with _cache_lock:
        _value_cache[key] = value
        _value_cache.move_to_end(key)
        while len(_value_cache) > VALUE_CACHE_SIZE:
            _value_cache.popitem(last=False)


def _fallback_value(segment_id: str, sk: str) -> float:
    key = (segment_id, sk)
    with _cache_lock:
        value = _value_cache.get(key, MISSING_VALUE)
        if key in _value_cache:
            _value_cache.move_to_end(key)
        return value


def _unique_in_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _fetch_batch(dynamodb, table_name: str, keys: list[dict]) -> list[dict]:
    """DynamoDB가 돌려준 미처리 키를 짧게 재시도한다."""

    items: list[dict] = []
    pending = keys
    for attempt in range(MAX_BATCH_GET_ATTEMPTS):
        response = dynamodb.batch_get_item(
            RequestItems={
                table_name: {
                    "Keys": pending,
                    "ConsistentRead": True,
                }
            }
        )
        items.extend(response.get("Responses", {}).get(table_name, []))
        pending = (
            response.get("UnprocessedKeys", {})
            .get(table_name, {})
            .get("Keys", [])
        )
        if not pending:
            break
        time.sleep(0.05 * (2 ** attempt))

    if pending:
        logger.warning("DynamoDB BatchGet 미처리 키: %s개", len(pending))
    return items


def get_type3_values(
    segment_ids: list[str],
    requested_at: datetime,
    *,
    dynamodb=None,
    table_name: str | None = None,
) -> list[float]:
    """DynamoDB를 조회하고 입력 segment 순서대로 숫자 값을 반환한다."""

    sk = build_sort_key(TYPE3_ID, requested_at)
    resolved_table = table_name or DYNAMODB_NAV_TABLE
    found: dict[str, float] = {}

    try:
        if not resolved_table:
            raise RuntimeError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")
        resource = dynamodb or get_dynamodb_resource()
        unique_segments = _unique_in_order(segment_ids)
        for offset in range(0, len(unique_segments), DYNAMODB_BATCH_SIZE):
            chunk = unique_segments[offset:offset + DYNAMODB_BATCH_SIZE]
            keys = [{"segment_id": segment_id, "sk": sk} for segment_id in chunk]
            for item in _fetch_batch(resource, resolved_table, keys):
                if "segment_id" not in item or "value" not in item:
                    continue
                segment_id = str(item["segment_id"])
                value = float(item["value"])
                found[segment_id] = value
                _remember_value(segment_id, sk, value)
    except Exception:
        logger.exception("DynamoDB Type 3 조회 실패; 캐시 또는 기본값으로 응답합니다")

    missing = sum(segment_id not in found for segment_id in segment_ids)
    if missing:
        logger.warning("Type 3 조회 누락: %s/%s", missing, len(segment_ids))

    return [
        found[segment_id]
        if segment_id in found
        else _fallback_value(segment_id, sk)
        for segment_id in segment_ids
    ]


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "dynamodb_table_configured": bool(DYNAMODB_NAV_TABLE),
    }


@app.post("/api/navigation/values", response_model=list[float])
def navigation_values(payload: NavigationValuesRequest) -> list[float]:
    segment_ids, _, requested_at = payload.root
    return get_type3_values(segment_ids, requested_at)
