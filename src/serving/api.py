"""Type3(승차 수) 서빙 조회 라이브러리.

원래는 이 모듈 자체가 독립 FastAPI 앱(및 Lambda)이었지만, 여러 타입의
Lambda를 nav_api.py 하나로 통합하면서(docs/superpowers/plans/
2026-08-23-unified-navigation-api.md) 실제 배포는 src/serving/lambda_handler.py
-> nav_api.py 경로만 쓰게 됐다. 이 모듈은 이제 nav_api.py가 가져다 쓰는
get_type3_values() 조회 로직만 담은 라이브러리다 - 자체 FastAPI 앱/엔드포인트는
어디서도 실행되지 않아 제거했다(2026-08-26).
"""

from __future__ import annotations

import re
import time
from datetime import datetime

import psycopg2
from psycopg2 import sql

from src.common import gold_snapshot
from src.common.config import (
    RDS_DB,
    RDS_HOST,
    RDS_PASSWORD,
    RDS_PORT,
    RDS_USER,
    SERVING_RDS_CONNECT_TIMEOUT_SECONDS,
    SERVING_RDS_STATEMENT_TIMEOUT_MS,
    SERVING_TABLE_TYPE3,
    TLC_TYPE3_DOW_NAMES,
    TYPE3_RDS_BATCH_SIZE,
)
from src.common.logger import get_logger
from src.common.tier_metrics import log_tier_summary
from src.common.utils import unique_in_order


logger = get_logger(__name__, log_to_file=True, log_file_stem="navigation_api")

WEEKDAY_NAMES = TLC_TYPE3_DOW_NAMES
# Type3 RDS는 (segment_id, dow, time)이 복합키인 flat 테이블이다(2026-08-24
# 스키마 개편 - 예전엔 세그먼트당 row 1개에 336개 값을 JSONB로 중첩했었다).
# 한 요청 안에서 dow/time은 요청 시각 하나로 고정되므로, segment_id
# 여러 개를 한 번에 조회할 때도 dow/time 조건은 쿼리에 딱 한 번만 걸면 된다.
TYPE3_BATCH_SIZE = TYPE3_RDS_BATCH_SIZE
MISSING_VALUE = 0.0

# RDS 커넥션 타임아웃/쿼리 타임아웃: 이 API는 Lambda 콜드/웜스타트 전체
# 시간 예산이 빠듯해서 RDS 커넥션/쿼리가 오래 걸리면 기다리는 대신 빨리
# 실패하고 fallback(S3 스냅샷/기본값)으로 넘어가야 한다. statement_timeout은
# 커넥션 세션 옵션으로 서버 쪽에 강제한다 — psycopg2 자체엔 botocore
# Config의 read_timeout과 동등한 클라이언트 옵션이 없어서, 쿼리 실행 시간
# 상한은 Postgres 서버가 직접 끊게 한다. nav_lookup.py(type1/2)와 같은
# RDS 인스턴스를 상대로 같은 이유로 쓰는 값이라 config.py에서 공유한다.
RDS_CONNECT_TIMEOUT_SECONDS = SERVING_RDS_CONNECT_TIMEOUT_SECONDS
RDS_STATEMENT_TIMEOUT_MS = SERVING_RDS_STATEMENT_TIMEOUT_MS

# RDS 자체가 응답 불가능할 때 쓰는 S3 스냅샷 2개(src/tlc/gold2.py/
# spark_jobs/tlc_pipeline_job.py의 _export_type3_snapshot이 RDS 쓰기
# 성공 시마다 갱신). zone→segment로 확장된 7,300만 행을 그대로 담으면
# 너무 커서, 확장 전 재료(zone 단위 rolling 평균 + segment→zone 매핑)만
# 담아뒀다가 조회 시 조합해서 재구성한다(무손실 - 확장이 단순 값 복사라서
# 가능함). 이 스냅샷은 프로세스당 최초 1회만 S3에서 읽어 메모리에 보관한다
# (재다운로드 방지용 로딩 캐시 - "최근 RDS 성공값"을 기억하는 캐시와는
# 성격이 다르다).
_type3_zone_snapshot = gold_snapshot.LazySnapshot("type3_zone")
_type3_mapping_snapshot = gold_snapshot.LazySnapshot("type3_mapping")


def _weekday_and_bucket(requested_at: datetime) -> tuple[str, str]:
    """요청 시각을 (요일, 30분 버킷) 쌍으로 변환한다.

    RDS 아이템 안의 중첩 JSON(`values[dow][bucket]`)에서 값을 꺼낼 때
    쓰는 실제 조회 키다."""

    slot_minute = (requested_at.minute // 30) * 30
    dow = WEEKDAY_NAMES[requested_at.weekday()]
    return dow, f"{requested_at.hour:02d}{slot_minute:02d}"


_db_connection = None


def get_db_connection():
    """RDS 커넥션을 Lambda 웜스타트 사이에 재사용한다. 끊어진
    채로 남아있으면(RDS 재부팅, 유휴 타임아웃 등) 자동으로 다시 연다 —
    콜드스타트가 아닌 첫 호출이 예외 대신 재연결로 복구되게 하기 위함."""
    global _db_connection

    if not SERVING_TABLE_TYPE3:
        raise RuntimeError("SERVING_TABLE_TYPE3 환경변수가 필요합니다")
    if not RDS_HOST or not RDS_DB:
        raise RuntimeError("RDS_HOST/RDS_DB 환경변수가 필요합니다")

    if _db_connection is not None and not _db_connection.closed:
        try:
            with _db_connection.cursor() as cur:
                cur.execute("SELECT 1")
            return _db_connection
        except psycopg2.Error:
            logger.warning("RDS 커넥션이 끊어져 재연결합니다")

    _db_connection = psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        connect_timeout=RDS_CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={RDS_STATEMENT_TIMEOUT_MS}",
    )
    _db_connection.autocommit = True
    return _db_connection


def _resolve_from_zone_snapshot(segment_id: str, dow: str, bucket: str) -> float | None:
    """Zone 스냅샷 + segment→zone 매핑으로 값을 재구성한다. 매핑에 그
    segment가 없거나 zone 스냅샷에 그 (zone, dow, bucket) 조합이 없으면
    None을 돌려줘서 호출부가 최종 기본값(0)으로 내려가게 한다.

    두 스냅샷의 .get()을 항상 함께 먼저 부른다(먼저 mapping만 보고
    zone_id가 없으면 조기 반환하지 않는다) - 두 파일은 항상 함께
    최초 1회 로드되어야 하는 짝이라, 매핑 미스가 잦은 요청 초반에
    zone 스냅샷 로드가 계속 미뤄지다 나중에서야 불쑥 S3를 부르는
    타이밍 편차를 없앤다."""
    mapping = _type3_mapping_snapshot.get()
    zone_snapshot = _type3_zone_snapshot.get()
    zone_id = mapping.get(segment_id)
    if zone_id is None:
        return None
    return zone_snapshot.get(f"{zone_id}#{dow}#{bucket}")


def _fallback_value(segment_id: str, dow: str, bucket: str) -> tuple[float, str]:
    """두 번째 반환값(tier)은 어떤 단계에서 값을 뽑았는지("snapshot"/
    "hardcoded") - Grafana 대시보드용 fallback 히트율 집계에 쓴다
    (get_type3_values 참고)."""
    value = _resolve_from_zone_snapshot(segment_id, dow, bucket)
    if value is not None:
        return value, "snapshot"
    return MISSING_VALUE, "hardcoded"


_TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")


def _fetch_batch(
    conn, table_name: str, segment_ids: list[str], dow: str, bucket: str
) -> list[tuple[str, float]]:
    """segment_id 목록 + (dow, time) 하나로 (segment_id, value) 쌍을
    한 번에 조회한다.

    DynamoDB BatchGetItem은 요청량 제한으로 일부만 처리하고 나머지를
    UnprocessedKeys로 돌려주는 부분 실패가 있어 재시도 루프가 필요했는데,
    SQL 쿼리 하나는 원자적으로 전체 성공/전체 실패이므로 그런 부분 실패
    자체가 없다 — 재시도 로직이 필요 없어졌다. 커넥션 문제(끊김 등)는 이
    함수를 호출하는 get_type3_values()의 try/except가 잡아서 S3 스냅샷/
    기본값 fallback으로 넘어간다."""

    if not _TABLE_NAME_PATTERN.match(table_name):
        raise ValueError(f"잘못된 테이블 이름입니다: {table_name}")

    query = sql.SQL(
        "SELECT segment_id, value FROM {table} "
        "WHERE segment_id = ANY(%s) AND dow = %s AND time = %s"
    ).format(table=sql.Identifier(table_name))

    with conn.cursor() as cur:
        cur.execute(query, (segment_ids, dow, bucket))
        return cur.fetchall()


def get_type3_values(
    segment_ids: list[str],
    requested_at: datetime,
    *,
    conn=None,
    table_name: str | None = None,
) -> list[float]:
    """RDS를 조회하고 입력 segment 순서대로 숫자 값을 반환한다. 값만 반환하는
    얇은 래퍼다 — 값의 출처 계층(rds/snapshot/hardcoded)까지 필요하면
    get_type3_values_with_tiers를 쓴다. 기존 호출부/테스트 하위 호환용."""

    values, _tiers = get_type3_values_with_tiers(
        segment_ids, requested_at, conn=conn, table_name=table_name
    )
    return values


def get_type3_values_with_tiers(
    segment_ids: list[str],
    requested_at: datetime,
    *,
    conn=None,
    table_name: str | None = None,
) -> tuple[list[float], list[str]]:
    """get_type3_values와 동일하되 values[i]가 어느 계층(rds/snapshot/hardcoded)
    에서 왔는지도 함께 반환한다. API 응답에 신뢰도를 노출할 때 쓴다
    (RELIABILITY_PRINCIPLES.md 원칙 0-1).

    Type3 테이블은 (segment_id, dow, time)이 복합키인 flat 테이블이라,
    요청 시각 하나로 정해지는 dow/time 조건과 segment_id 목록으로 바로
    필요한 값만 조회한다."""

    dow, bucket = _weekday_and_bucket(requested_at)
    resolved_table = table_name or SERVING_TABLE_TYPE3
    found: dict[str, float] = {}

    try:
        if not resolved_table:
            raise RuntimeError("SERVING_TABLE_TYPE3 환경변수가 필요합니다")
        connection = conn or get_db_connection()
        unique_segments = unique_in_order(segment_ids)
        start = time.perf_counter()
        for offset in range(0, len(unique_segments), TYPE3_BATCH_SIZE):
            chunk = unique_segments[offset:offset + TYPE3_BATCH_SIZE]
            for segment_id, value in _fetch_batch(connection, resolved_table, chunk, dow, bucket):
                if value is None:
                    continue
                segment_id = str(segment_id)
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    logger.exception(
                        f"RDS Type 3 값 형식 오류(다음 fallback으로 넘어감): "
                        f"segment_id={segment_id} dow={dow} bucket={bucket}"
                    )
                    continue
                found[segment_id] = value
        elapsed_ms = (time.perf_counter() - start) * 1000
        # db.py의 batch_get_items()와 같은 형식 - Grafana의 "타입별 RDS 쿼리
        # 응답시간" 패널이 table 필드로 두 경로를 같이 묶어서 집계한다
        # (Type3는 db.py를 안 거치는 별도 쿼리 경로라 여기서 따로 남겨야 함).
        logger.info(f"[rds_query_duration] table={resolved_table} ms={elapsed_ms:.1f}")
    except Exception:
        logger.exception("RDS Type 3 조회 실패; S3 스냅샷 또는 기본값으로 응답합니다")

    missing = sum(segment_id not in found for segment_id in segment_ids)
    if missing:
        logger.warning("Type 3 조회 누락: %s/%s", missing, len(segment_ids))

    tiers: list[str] = []
    values = []
    for segment_id in segment_ids:
        if segment_id in found:
            tiers.append("rds")
            values.append(found[segment_id])
        else:
            value, tier = _fallback_value(segment_id, dow, bucket)
            tiers.append(tier)
            values.append(value)

    # 요청당 한 번만 남긴다(세그먼트별로 남기면 요청 하나에 수백 줄이 될 수
    # 있음) - CloudWatch Logs Insights가 이 로그를 집계해서 Grafana의
    # "Type3 fallback 계층 비율" 패널을 그린다.
    log_tier_summary(logger, "type3_fallback_tier_summary", tiers, ["rds", "snapshot", "hardcoded"])

    return values, tiers
