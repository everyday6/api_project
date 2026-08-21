"""
세그먼트 지표 조회 + fallback 체인

"무조건 응답"(설계 문서 7절)을 구현하는 핵심 모듈. 키가 없는 경우와
DynamoDB 호출 자체가 실패(예외)하는 경우를 구분하지 않고 똑같이 다음
fallback 단계로 넘어간다.

체인 순서(고정):
  1. 정확한 (segment_id, bucket) 값
  2. (type1만) (segment_id, "AVG")
  3. (GLOBAL, "DEFAULT") — 배포 시점에 수동으로 심어둔 고정값
  4. 코드 상수 — 외부 호출이 전혀 없는 최후의 보루
"""

from __future__ import annotations

from src.common.config import (
    AVG_SORT_KEY,
    BUCKET_MINUTES,
    DEFAULT_SORT_KEY,
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    GLOBAL_PARTITION_KEY,
    LENGTH_SORT_KEY,
)
from src.common.dynamodb import batch_get_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_lookup")

# DynamoDB/GLOBAL#DEFAULT까지 전부 실패했을 때 쓰는 최후의 상수. 외부 호출이
# 전혀 없어 DynamoDB 리전 자체가 완전히 응답 불가능한 상황에서도 동작한다.
# TODO(팀 검토 필요): scripts/seed_dynamodb_defaults.py의 기본값과 동일한
# 정성적 초안.
_HARDCODED_DEFAULTS = {1: 45, 2: 300}


def time_to_bucket(time_str: str) -> str:
    """'HH:MM' -> 'HHMM' (30분 단위로 내림)."""
    hour_str, minute_str = time_str.split(":")
    bucket_minute = (int(minute_str) // BUCKET_MINUTES) * BUCKET_MINUTES
    return f"{int(hour_str):02d}{bucket_minute:02d}"


def table_for_type(type_: int) -> str:
    if type_ == 1:
        return DYNAMODB_TABLE_TYPE1
    if type_ == 2:
        return DYNAMODB_TABLE_TYPE2
    raise ValueError(f"알 수 없는 type: {type_}")


def _resolve_tier(resolved: dict[str, int], ids: list[str], table_name: str, sk: str) -> None:
    """아직 못 찾은 segment_id들에 대해 (segment_id, sk) 키로 조회해서
    찾은 만큼 resolved에 채운다. DynamoDB 호출 자체가 실패해도 예외를
    삼키고 로그만 남긴다(호출부가 다음 fallback 단계로 계속 진행)."""
    if not ids:
        return

    keys = [{"segment_id": sid, "sk": sk} for sid in ids]

    try:
        items = batch_get_items(table_name, keys)
    except Exception:
        logger.exception(f"DynamoDB batch_get_items 실패: table={table_name} sk={sk}")
        return

    for sid in ids:
        item = items.get((sid, sk))
        if item is None:
            continue
        try:
            resolved[sid] = int(item["value"])
        except (KeyError, ValueError, TypeError):
            logger.exception(
                f"DynamoDB 항목 형식 오류(다음 fallback 단계로 넘어감): "
                f"table={table_name} sk={sk} segment_id={sid}"
            )


def _lookup_global_default(table_name: str, type_: int) -> int:
    try:
        items = batch_get_items(table_name, [{"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY}])
        item = items.get((GLOBAL_PARTITION_KEY, DEFAULT_SORT_KEY))
        if item is not None:
            return int(item["value"])
    except Exception:
        logger.exception(f"DynamoDB GLOBAL#DEFAULT 조회 실패: table={table_name}")

    logger.warning(f"GLOBAL#DEFAULT까지 실패 -> 코드 상수 사용: type={type_}")
    return _HARDCODED_DEFAULTS[type_]


def resolve_segment_values(segment_ids: list[str], type_: int, time_str: str) -> list[int]:
    """요청받은 segment_ids 순서(중복 포함)대로 값을 반환한다. 항상 길이가
    같은 리스트를 반환한다 — 절대 예외를 던지지 않는다(이 함수가 최후의
    방어선이다 — 상위에 입력 검증 레이어가 없어도 안전해야 한다)."""
    try:
        return _resolve_segment_values_inner(segment_ids, type_, time_str)
    except Exception:
        logger.exception(
            f"resolve_segment_values 예상치 못한 오류 - 코드 상수로 응답: "
            f"type={type_} time={time_str}"
        )
        fallback_value = _HARDCODED_DEFAULTS.get(type_, _HARDCODED_DEFAULTS[1])
        return [fallback_value] * len(segment_ids)


def _resolve_segment_values_inner(segment_ids: list[str], type_: int, time_str: str) -> list[int]:
    table_name = table_for_type(type_)

    # Type에 따라 첫 번째 tier에서 사용할 sort key 결정
    # Type1: 시간 버킷 (HH:MM), Type2: 고정값 "LENGTH"
    if type_ == 1:
        first_tier_sk = time_to_bucket(time_str)
    else:  # type_ == 2
        first_tier_sk = LENGTH_SORT_KEY

    unique_ids = list(dict.fromkeys(segment_ids))
    resolved: dict[str, int] = {}

    # 1단계: 정확한 값 (Type1은 버킷, Type2는 LENGTH)
    _resolve_tier(resolved, unique_ids, table_name, first_tier_sk)

    remaining = [sid for sid in unique_ids if sid not in resolved]

    # 2단계(type1만): 세그먼트 전체 평균
    if remaining and type_ == 1:
        _resolve_tier(resolved, remaining, table_name, AVG_SORT_KEY)
        remaining = [sid for sid in unique_ids if sid not in resolved]

    # 3~4단계: GLOBAL#DEFAULT, 없으면 코드 상수
    if remaining:
        default_value = _lookup_global_default(table_name, type_)
        for sid in remaining:
            resolved[sid] = default_value

    return [resolved[sid] for sid in segment_ids]
