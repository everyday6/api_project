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

Type1(소요시간)의 segment_ids는 경로를 순서대로 나열한 것으로 간주한다.
요청 시각은 첫 세그먼트에만 그대로 쓰고, 이후 세그먼트는 앞 세그먼트들의
누적 소요시간만큼 시각을 이동해서 조회한다(_resolve_time_values 참고).
Type2(길이)는 시간과 무관해 순서/중복에 영향받지 않는다.
"""

from __future__ import annotations

import time

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

# type1은 세그먼트마다 순차로 DynamoDB를 조회하므로(누적시각 때문에 배치 불가),
# DynamoDB 리전 전체가 응답 불가능한 상황에서는 세그먼트 수만큼 호출이 전부
# 느리게 실패하며 쌓여 응답 자체가 타임아웃날 수 있다. 연속으로 이 횟수만큼
# 호출 자체가 실패하면 남은 세그먼트는 DynamoDB를 더 안 건드리고 곧바로
# 코드 상수로 채운다.
_CIRCUIT_BREAKER_THRESHOLD = 3

# 실패가 아니라 "다 성공은 하는데 순차 호출이 너무 많이 쌓이는" 경우를 막는
# 시간 예산. 세그먼트 500개(허용 상한)로 실측했을 때 Lambda 타임아웃(당시
# 10초)을 실제로 넘겨서 500 Internal Server Error가 나는 걸 확인했다 -
# 실패 기반 circuit breaker만으로는 이 경우(호출은 다 성공하지만 느림)를
# 못 막는다. 남은 시간이 얼마 안 되면 성공/실패와 무관하게 회로를 열어
# 남은 세그먼트를 코드 상수로 채운다 - 정확도보다 "무조건 응답"이 우선이다.
#
# 이 값은 Lambda 콘솔의 타임아웃 설정과 자동으로 연동되지 않는다 - 타임아웃을
# 바꾸면 이 값도 사람이 직접 같이 조정해야 한다. Lambda 타임아웃을 10초 ->
# 15초로 올린 뒤, 회로를 연 다음 나머지 세그먼트 채우기+응답 직렬화에 걸리는
# 시간(실측 약 0.3초) 감안해서 4초 버퍼를 남기고 11초로 올렸다.
# TODO(팀 검토 필요): 정성적 값 - Lambda 타임아웃을 또 바꾸면 이 값도 같이 검토.
_TIME_BUDGET_SECONDS = 11.0


def time_to_bucket(time_str: str) -> str:
    """'HH:MM' -> 'HHMM' (30분 단위로 내림)."""
    hour_str, minute_str = time_str.split(":")
    bucket_minute = (int(minute_str) // BUCKET_MINUTES) * BUCKET_MINUTES
    return f"{int(hour_str):02d}{bucket_minute:02d}"


def _add_seconds(time_str: str, seconds: int) -> str:
    """'HH:MM'에 초를 더해 다시 'HH:MM'으로 반환한다. 하루(86400초)를 넘기면
    24시간으로 wrap한다 - 버킷 조회에는 시:분만 필요해서 날짜는 추적하지
    않는다."""
    hour_str, minute_str = time_str.split(":")
    total_seconds = (int(hour_str) * 3600 + int(minute_str) * 60 + seconds) % 86400
    new_hour, remainder_seconds = divmod(total_seconds, 3600)
    new_minute = remainder_seconds // 60
    return f"{new_hour:02d}:{new_minute:02d}"


def table_for_type(type_: int) -> str:
    if type_ == 1:
        return DYNAMODB_TABLE_TYPE1
    if type_ == 2:
        return DYNAMODB_TABLE_TYPE2
    raise ValueError(f"알 수 없는 type: {type_}")


def _resolve_tier(resolved: dict[str, int], ids: list[str], table_name: str, sk: str) -> bool:
    """아직 못 찾은 segment_id들에 대해 (segment_id, sk) 키로 조회해서
    찾은 만큼 resolved에 채운다. DynamoDB 호출 자체가 실패해도 예외를
    삼키고 로그만 남긴다(호출부가 다음 fallback 단계로 계속 진행).

    반환값은 "값을 찾았는지"가 아니라 "DynamoDB 호출 자체가 성공했는지"다
    (circuit breaker가 장애와 단순 미스를 구분하는 데 씀)."""
    if not ids:
        return True

    keys = [{"segment_id": sid, "sk": sk} for sid in ids]

    try:
        items = batch_get_items(table_name, keys)
    except Exception:
        logger.exception(f"DynamoDB batch_get_items 실패: table={table_name} sk={sk}")
        return False

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

    return True


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

    if type_ == 1:
        return _resolve_time_values(segment_ids, table_name, time_str)
    return _resolve_length_values(segment_ids, table_name)


def _resolve_length_values(segment_ids: list[str], table_name: str) -> list[int]:
    """Type2(길이)는 시간과 무관해 세그먼트당 값이 하나뿐이다 - 중복
    segment_id는 한 번만 조회해서 재사용해도 안전하다."""
    unique_ids = list(dict.fromkeys(segment_ids))
    resolved: dict[str, int] = {}

    _resolve_tier(resolved, unique_ids, table_name, LENGTH_SORT_KEY)
    remaining = [sid for sid in unique_ids if sid not in resolved]

    if remaining:
        default_value = _lookup_global_default(table_name, 2)
        for sid in remaining:
            resolved[sid] = default_value

    return [resolved[sid] for sid in segment_ids]


def _resolve_time_values(segment_ids: list[str], table_name: str, time_str: str) -> list[int]:
    """Type1(소요시간)은 segment_ids를 경로 순서로 간주한다. 세그먼트 k의
    조회 시각은 "요청 시각 + 세그먼트 1..k-1의 소요시간 합"이다 - 그
    세그먼트에 실제로 도착하는 시점의 버킷을 봐야 하기 때문이다. 이 누적
    시각은 앞 세그먼트의 조회 *결과*에 의존하므로 순서대로(순차) 처리해야
    하고, 같은 segment_id가 경로에 두 번 나와도(루프) 등장 위치의 누적
    시각이 다르면 값도 다를 수 있어 중복 제거를 하지 않는다.

    GLOBAL#DEFAULT는 요청 전체에서 값이 불변이므로 한 번만 조회해서
    재사용한다(세그먼트마다 다시 조회하지 않음). 또한 DynamoDB 호출이
    연속으로 실패하면(circuit breaker) 남은 세그먼트는 더 이상 DynamoDB를
    건드리지 않고 코드 상수로 바로 채운다 - 안 그러면 리전 전체 장애 시
    세그먼트 수만큼 느린 실패가 순차로 쌓여 응답이 타임아웃날 수 있다."""
    values: list[int] = []
    elapsed_seconds = 0
    consecutive_failures = 0
    circuit_open = False
    cached_default: int | None = None
    start_time = time.monotonic()

    def get_default() -> int:
        nonlocal cached_default
        if cached_default is None:
            cached_default = _lookup_global_default(table_name, 1)
        return cached_default

    for sid in segment_ids:
        if not circuit_open and time.monotonic() - start_time >= _TIME_BUDGET_SECONDS:
            circuit_open = True
            logger.warning(
                f"시간 예산({_TIME_BUDGET_SECONDS}초) 초과 -> circuit open, "
                f"남은 세그먼트는 코드 상수로 응답: table={table_name}"
            )

        if circuit_open:
            value = _HARDCODED_DEFAULTS[1]
            values.append(value)
            elapsed_seconds += value
            continue

        lookup_time = _add_seconds(time_str, elapsed_seconds)
        bucket = time_to_bucket(lookup_time)

        resolved: dict[str, int] = {}
        call_ok = _resolve_tier(resolved, [sid], table_name, bucket)

        if sid not in resolved:
            call_ok = _resolve_tier(resolved, [sid], table_name, AVG_SORT_KEY) and call_ok

        if sid in resolved:
            consecutive_failures = 0
            value = resolved[sid]
        else:
            if call_ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= _CIRCUIT_BREAKER_THRESHOLD:
                    circuit_open = True
                    logger.warning(
                        f"DynamoDB 호출 {_CIRCUIT_BREAKER_THRESHOLD}회 연속 실패 -> "
                        f"circuit open, 남은 세그먼트는 코드 상수로 응답: table={table_name}"
                    )
            value = get_default()

        values.append(value)
        elapsed_seconds += value

    return values
