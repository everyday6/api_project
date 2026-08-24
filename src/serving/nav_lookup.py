"""
세그먼트 지표 조회 + fallback 체인

"무조건 응답"(설계 문서 7절)을 구현하는 핵심 모듈. 키가 없는 경우와
DB 호출 자체가 실패(예외)하는 경우를 구분하지 않고 똑같이 다음 fallback
단계로 넘어간다.

Type1(시간)은 RDS Postgres(src/common/rds.py)로 서빙한다. DynamoDB에서
RDS로 옮기며 멀티 AZ 자동 failover(가용성)를 잃은 걸 보완하려고, RDS
"자체가 응답 불가능한" 경우를 위한 별도 폴백 계층(메모리 캐시 -> S3
Gold 스냅샷)을 추가로 둔다:

  [RDS 정상 응답]
  1. Fresh Exact — 정확한 (segment_id, bucket) 값. observed_at이
     freshness 기준(_FRESHNESS_THRESHOLD_SECONDS) 이내인 경우만 채택.
  2. Historical AVG — (segment_id, "AVG")
  3. SPEC Estimate — (segment_id, "SPEC"). 도로 스펙(length_ft / speed_limit_mph)
     으로 계산한 추정 통과시간 — segment_length_pipeline이 LION 갱신 때마다
     (quarterly) 다시 써둔다.
  4. 코드 상수

  [RDS 자체가 응답 불가능 — 연결 실패/타임아웃]
  1. 메모리 캐시(이 프로세스가 이전에 RDS에서 성공적으로 읽은 값)
  2. S3 Gold 스냅샷(src/common/gold_snapshot.py) — RDS가 정상일 때 Gold
     파이프라인이 미리 내보내둔 세그먼트별 (exact, avg, spec) 스냅샷을
     처음 미스가 날 때 한 번만 통째로 로드해 메모리에 얹는다. 이 안에서도
     exact -> avg -> spec 순서로, 같은 freshness 기준을 재적용한다.
  3. 코드 상수

Type2(길이)도 RDS(segment_metrics_type2)로 서빙한다(별개 테이블,
시간 무관 정적값): 정확한 segment_id 값 → 코드 상수. 길이는 시간과
무관해 순서/중복에 영향받지 않고, GLOBAL#DEFAULT 같은 전역 폴백 행도
따로 안 둔다(정성적 초안값일 뿐이라 DB에 둘 이유가 약함 - type1처럼
멀티 AZ 손실을 보완하는 메모리/S3 폴백 계층도 아직은 안 둔다).

Type1(소요시간)의 segment_ids는 경로를 순서대로 나열한 것으로 간주한다.
요청 시각은 첫 세그먼트에만 그대로 쓰고, 이후 세그먼트는 앞 세그먼트들의
누적 소요시간만큼 시각을 이동해서 조회한다. 다만 어떤 버킷(sk)이 필요할지는
그 누적시각을 계산해봐야 알 수 있어서, RDS 조회 자체는 요청당 딱 한 번만
한다 - segment_id별로 존재 가능한 행이 PK(segment_id, sk) 특성상 최대
50개(버킷 48개+AVG+SPEC, sk가 날짜가 아니라 시간대라서)로 정해져 있어
필요할지 모르는 버킷까지 전부 미리 배치로 가져와도 데이터량이 작다. 그
결과로 순차 누적시각 계산 자체는 이 로컬 dict만 보고 끝나서(RDS를 더
안 건드림), "세그먼트 수만큼 RDS 왕복이 쌓여 응답이 느려지거나 타임아웃
나는" 문제 자체가 없어진다(_resolve_time_values 참고).
"""

from __future__ import annotations

import time

from src.common import gold_snapshot, rds
from src.common.config import BUCKET_MINUTES, RDS_TABLE_TYPE1, RDS_TABLE_TYPE2
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_lookup")

# RDS/DynamoDB/GLOBAL#DEFAULT까지 전부 실패했을 때 쓰는 최후의 상수. 외부
# 호출이 전혀 없어 어떤 저장소도 완전히 응답 불가능한 상황에서도 동작한다.
# TODO(팀 검토 필요): scripts/seed_dynamodb_defaults.py의 기본값과 동일한
# 정성적 초안.
_HARDCODED_DEFAULTS = {1: 45, 2: 300}

# type1 exact(segment_id, bucket) 값의 신선도 기준(초). observed_at이 이보다
# 오래되면 그 bucket이 갱신을 멈췄다고 보고(파이프라인 중단 등) Historical
# AVG로 내려간다. DB TTL로 삭제하지 않고 조회 시점에 매번 판단한다. RDS
# 폴백(S3 스냅샷) 안 exact 값에도 같은 기준을 그대로 적용한다.
# TODO(팀 검토 필요): 아직 기준 미확정 - 30분 수집 주기의 2배인 1시간으로
# 잡은 정성적 초안. observed_at은 그 bucket 값을 마지막으로 계산한 시각
# (epoch seconds) - src/nav_time/gold2.py가 기록한다.
_FRESHNESS_THRESHOLD_SECONDS = 3600.0

# RDS 장애 시 쓰는 메모리 캐시. segment_id -> {"avg", "spec", "exact_value",
# "exact_observed_at"}. 상한을 안 걸면 Lambda 인스턴스가 오래 켜져 있을 때
# 계속 커져서 OOM으로 함수 자체가 죽을 수 있어(관측값이 없는 것보다 훨씬
# 나쁜 실패) 개수 상한 + 가장 오래된 것부터 제거한다.
_MEMORY_CACHE_MAX_SIZE = 50_000
_memory_cache: dict[str, dict] = {}

# S3 Gold 스냅샷은 이 프로세스(Lambda 웜 인스턴스)에서 처음 필요할 때
# 딱 한 번만 통째로 읽는다 - 세그먼트마다 매번 S3를 부르면 RDS가 죽어있는
# 동안 세그먼트 수만큼 S3 호출이 쌓이는 문제가 재발한다.
_s3_snapshot_loaded = False
_s3_snapshot: dict[str, dict] = {}


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
        return RDS_TABLE_TYPE1
    if type_ == 2:
        return RDS_TABLE_TYPE2
    raise ValueError(f"알 수 없는 type: {type_}")


def _is_fresh(observed_at) -> bool:
    """observed_at(epoch seconds)이 freshness 기준 이내인지 확인한다.
    없거나 형식이 이상하면(레거시 데이터 등) 안전한 쪽으로 "신선하지
    않음"으로 처리해 다음 단계로 내려가게 한다."""
    try:
        observed_at = float(observed_at)
    except (TypeError, ValueError):
        return False
    return (time.time() - observed_at) <= _FRESHNESS_THRESHOLD_SECONDS


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
    segment_id는 한 번만 조회해서 재사용해도 안전하다. RDS 호출 자체가
    실패하면(연결 장애 등) GLOBAL#DEFAULT 같은 중간 단계 없이 바로 코드
    상수로 응답한다 - type1과 달리 메모리/S3 폴백 계층은 아직 없다."""
    unique_ids = list(dict.fromkeys(segment_ids))

    try:
        rows_by_segment = rds.batch_get_static_values(table_name, unique_ids)
    except Exception:
        logger.exception(f"RDS 배치 조회 실패 -> 코드 상수로 응답: table={table_name}")
        rows_by_segment = {}

    resolved = {
        sid: round(rows_by_segment[sid]["value"]) if sid in rows_by_segment else _HARDCODED_DEFAULTS[2]
        for sid in unique_ids
    }
    return [resolved[sid] for sid in segment_ids]


def _remember_in_memory(segment_id: str, entry: dict) -> None:
    _memory_cache[segment_id] = entry
    if len(_memory_cache) > _MEMORY_CACHE_MAX_SIZE:
        oldest_key = next(iter(_memory_cache))
        del _memory_cache[oldest_key]


def _load_s3_snapshot_once() -> None:
    global _s3_snapshot_loaded, _s3_snapshot
    if _s3_snapshot_loaded:
        return
    _s3_snapshot = gold_snapshot.read_snapshot("type1")
    _s3_snapshot_loaded = True


def _resolve_from_fallback(segment_id: str) -> int:
    """RDS 자체가 응답 불가능할 때: 메모리 캐시 -> S3 스냅샷(최초 미스 때
    한 번만 로드) -> 코드 상수 순서로 내려간다. 스냅샷 안에서도 exact(신선한
    경우만) -> avg -> spec 순서를 그대로 재현한다."""
    entry = _memory_cache.get(segment_id)

    if entry is None:
        _load_s3_snapshot_once()
        entry = _s3_snapshot.get(segment_id)
        if entry:
            _remember_in_memory(segment_id, entry)

    if entry:
        exact_value = entry.get("exact_value")
        if exact_value is not None and _is_fresh(entry.get("exact_observed_at")):
            return round(exact_value)
        if entry.get("avg") is not None:
            return round(entry["avg"])
        if entry.get("spec") is not None:
            return round(entry["spec"])

    logger.warning(f"메모리 캐시/S3 스냅샷에도 값 없음 -> 코드 상수 사용: segment_id={segment_id}")
    return _HARDCODED_DEFAULTS[1]


def _resolve_time_values(segment_ids: list[str], table_name: str, time_str: str) -> list[int]:
    """Type1(소요시간)은 segment_ids를 경로 순서로 간주한다. 세그먼트 k의
    조회 시각은 "요청 시각 + 세그먼트 1..k-1의 소요시간 합"이다 - 그
    세그먼트에 실제로 도착하는 시점의 버킷을 봐야 하기 때문이다. 이 누적
    시각은 앞 세그먼트의 조회 *결과*에 의존하므로 순서대로(순차) 처리해야
    하고, 같은 segment_id가 경로에 두 번 나와도(루프) 등장 위치의 누적
    시각이 다르면 값도 다를 수 있어 중복 제거를 하지 않는다.

    RDS 조회는 요청당 딱 한 번(batch_resolve_type1_rows)만 하고, 이후
    누적시각 계산은 그 결과 dict만 보고 순수 로컬 연산으로 끝낸다 - 그
    한 번의 호출이 실패하면(RDS 자체 장애) 모든 세그먼트를 메모리/S3
    폴백으로 채운다. RDS 호출이 요청당 하나뿐이라 "연속 실패"나 "순차
    호출이 쌓여 느려짐" 같은 문제 자체가 없어져 별도 circuit
    breaker/시간 예산이 필요 없다."""
    try:
        rows_by_segment = rds.batch_resolve_type1_rows(segment_ids, table_name)
    except Exception:
        logger.exception(f"RDS 배치 조회 실패 -> 전체 세그먼트 메모리/S3 폴백으로 응답: table={table_name}")
        return [_resolve_from_fallback(sid) for sid in segment_ids]

    values: list[int] = []
    elapsed_seconds = 0

    for sid in segment_ids:
        lookup_time = _add_seconds(time_str, elapsed_seconds)
        bucket = time_to_bucket(lookup_time)

        rows = rows_by_segment.get(sid, {})
        exact = rows.get(bucket)
        avg = rows.get("AVG")
        spec = rows.get("SPEC")

        if exact is not None and _is_fresh(exact["observed_at"]):
            value = round(exact["value"])
        elif avg is not None:
            value = round(avg["value"])
        elif spec is not None:
            value = round(spec["value"])
        else:
            value = _HARDCODED_DEFAULTS[1]

        # RDS가 정상 응답했으니, 다음 장애에 대비해 이 세그먼트의 최신
        # 상태를 메모리 캐시에 남겨둔다(폴백 경로와 같은 모양의 항목).
        _remember_in_memory(sid, {
            "avg": avg["value"] if avg is not None else None,
            "spec": spec["value"] if spec is not None else None,
            "exact_value": exact["value"] if exact is not None else None,
            "exact_observed_at": exact["observed_at"] if exact is not None else None,
        })

        values.append(value)
        elapsed_seconds += value

    return values
