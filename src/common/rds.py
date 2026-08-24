"""
RDS Postgres 서빙 클라이언트 — nav-gold type1 값 저장/조회.

DynamoDB에서 RDS로 옮기면서 멀티 AZ 자동 failover(가용성)를 잃는다. 이
모듈은 그 손실을 "빠르게 실패해서 호출부가 다음 폴백 단계(memory/S3
스냅샷)로 넘어가게" 하는 것으로 보완한다 — 그래서 연결/쿼리 실패를 여기서
삼키지 않고 그대로 던진다. "행이 없음"(정상 - 그 세그먼트에 그 tier 값이
없을 뿐)과 "RDS 자체가 응답 안 함"(비정상 - 폴백 체인 전체를 타야 함)을
호출부(src/serving/nav_lookup.py)가 구분해야 하기 때문이다.

커넥션은 프로세스(Lambda 웜 인스턴스)당 하나만 만들어 재사용한다 - 매
요청마다 새로 연결하면 TCP/TLS 핸드셰이크 비용이 매번 들어간다. 재사용
중인 커넥션이 끊겨 있으면(RDS 재시작 등) 다음 호출에서 예외가 나고,
그 시점에 캐시를 버리고 다시 연결을 시도한다.
"""

from __future__ import annotations

import psycopg2

from src.common.config import AVG_SORT_KEY, SPEC_SORT_KEY, get_rds_dsn
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="rds")

# RDS가 느려지거나 죽었을 때 요청이 오래 매달리지 않고 빠르게 실패해서
# 다음 폴백 단계(memory/S3)로 넘어가야 한다 - src/serving/api.py가
# DynamoDB 클라이언트에 쓰는 것과 같은 목적의 타임아웃.
_CONNECT_TIMEOUT_SECONDS = 2
_STATEMENT_TIMEOUT_MS = 2000

_connection = None


def get_connection():
    """캐시된 커넥션을 반환한다. 없거나 끊겨 있으면 새로 연결한다."""
    global _connection

    if _connection is not None and _connection.closed == 0:
        return _connection

    _connection = psycopg2.connect(
        get_rds_dsn(),
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
    )
    _connection.autocommit = True
    return _connection


def _reset_connection() -> None:
    """연결 자체가 문제였을 가능성이 있을 때 캐시를 버린다 - 다음 호출이
    새 연결을 시도하게 한다."""
    global _connection
    _connection = None


def ensure_table(table_name: str) -> None:
    """테이블이 없으면 만든다. 로컬 개발 편의용이다 - 실 RDS는 배포 시점에
    미리 만들어두고 운영 중에는 이 함수를 안 쓴다(실수로 스키마를 바꾸는
    걸 막기 위함, src/common/dynamodb.py의 ensure_table과 동일한 원칙)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    segment_id TEXT NOT NULL,
                    sk TEXT NOT NULL,
                    value NUMERIC NOT NULL,
                    observed_at DOUBLE PRECISION,
                    collected_date DATE,
                    count INTEGER,
                    PRIMARY KEY (segment_id, sk)
                )
            """)
    except psycopg2.OperationalError:
        _reset_connection()
        raise


def batch_resolve_type1_rows(segment_ids: list[str], table_name: str) -> dict[str, dict[str, dict]]:
    """요청에 포함된 segment_id 전체의 존재 가능한 모든 행(버킷 sk 최대
    48개 + AVG + SPEC)을 한 번의 쿼리로 가져온다.

    type1은 세그먼트별로 조회해야 할 버킷(sk)이 앞 세그먼트들의 누적
    소요시간에 따라 달라져서 요청을 받는 시점엔 어떤 버킷이 필요할지
    미리 알 수 없다 - 그런데 sk가 날짜가 아니라 시간대(하루 48개 버킷)라
    PK(segment_id, sk) 특성상 세그먼트 하나가 가질 수 있는 행은 최대
    50개(48버킷+AVG+SPEC)로 정해져 있다. 그래서 필요할지 모르는 버킷을
    미리 다 받아둬도 세그먼트당 데이터량이 작고, 이후 순차 누적시각
    계산(src/serving/nav_lookup.py._resolve_time_values)은 이 로컬 dict만
    보고 끝낼 수 있어 세그먼트 수만큼 RDS 왕복이 쌓이는 문제 자체가
    없어진다(예전엔 세그먼트당 1회씩 순차 호출 -> circuit breaker/time
    budget으로 방어해야 했는데, 이제 요청당 RDS 호출이 이 한 번뿐이라 그
    방어 장치 자체가 필요 없어졌다).

    반환값은 segment_id -> {sk: {"value", "observed_at"}} 매핑이고, 없는
    segment_id/sk는 키 자체가 없다. RDS 연결/쿼리 실패는 삼키지 않고 그대로
    던진다 - 호출부가 이걸로 "RDS 자체가 죽었다"를 판단해서 memory/S3
    폴백으로 넘어간다."""
    if not segment_ids:
        return {}

    unique_ids = list(dict.fromkeys(segment_ids))
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT segment_id, sk, value, observed_at FROM {table_name} "
                f"WHERE segment_id = ANY(%s)",
                (unique_ids,),
            )
            rows = cur.fetchall()
    except psycopg2.OperationalError:
        _reset_connection()
        raise

    result: dict[str, dict[str, dict]] = {}
    for segment_id, sk, value, observed_at in rows:
        result.setdefault(segment_id, {})[sk] = {"value": float(value), "observed_at": observed_at}
    return result


def upsert_items(items: list[dict], table_name: str) -> int:
    """여러 아이템을 upsert한다. item은 최소 {segment_id, sk, value}를
    포함해야 하고, observed_at/collected_date/count는 선택이다.

    Gold 파이프라인이 세그먼트 수천~수십만 건을 한 번에 쓸 때 쓴다 -
    execute_values로 한 번의 왕복에 여러 행을 보낸다."""
    if not items:
        return 0

    from psycopg2.extras import execute_values

    conn = get_connection()
    rows = [
        (
            item["segment_id"],
            item["sk"],
            item["value"],
            item.get("observed_at"),
            item.get("collected_date"),
            item.get("count"),
        )
        for item in items
    ]

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} (segment_id, sk, value, observed_at, collected_date, count)
                VALUES %s
                ON CONFLICT (segment_id, sk) DO UPDATE SET
                    value = EXCLUDED.value,
                    observed_at = EXCLUDED.observed_at,
                    collected_date = EXCLUDED.collected_date,
                    count = EXCLUDED.count
                """,
                rows,
            )
    except psycopg2.OperationalError:
        _reset_connection()
        raise

    return len(items)


def batch_get_rows(table_name: str, keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """(segment_id, sk) 키 목록으로 여러 행을 한 번에 조회한다. Gold
    파이프라인의 증분 갱신(예: AVG 계산)이 "기존 값"을 한꺼번에 읽을 때
    쓴다 - 서빙 경로(batch_resolve_type1_rows)와는 다른 호출부."""
    if not keys:
        return {}

    from psycopg2.extras import execute_values

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            rows = execute_values(
                cur,
                f"""
                SELECT t.segment_id, t.sk, t.value, t.count
                FROM {table_name} t
                JOIN (VALUES %s) AS k(segment_id, sk)
                    ON t.segment_id = k.segment_id AND t.sk = k.sk
                """,
                keys,
                fetch=True,
            )
    except psycopg2.OperationalError:
        _reset_connection()
        raise

    return {(sid, sk): {"value": float(value), "count": count} for sid, sk, value, count in rows}


def export_snapshot_source(table_name: str) -> dict[str, dict]:
    """S3 Gold 스냅샷(src/common/gold_snapshot.py)에 실어보낼 원본을 RDS에서
    뽑는다. 세그먼트당 AVG/SPEC/가장 최근 exact 값만 뽑는다 - exact는
    DISTINCT ON으로 세그먼트당 가장 최근 관측치 하나만 가져온다(48개 버킷
    전부는 필요 없다, 스냅샷을 쓰는 시점엔 오래된 건 이미 freshness 기준을
    넘겨 못 쓰므로 최신 1개면 충분하다).

    Gold 파이프라인(정상적인 RDS 상태에서만 도는 쓰기 경로)이 부르는
    함수라 실패 시 예외를 그대로 던져도 된다 - 서빙 경로의 "빠른 실패"
    요구사항과 다르다."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"SELECT segment_id, value FROM {table_name} WHERE sk = %s", (AVG_SORT_KEY,))
        avg_rows = cur.fetchall()

        cur.execute(f"SELECT segment_id, value FROM {table_name} WHERE sk = %s", (SPEC_SORT_KEY,))
        spec_rows = cur.fetchall()

        cur.execute(
            f"""
            SELECT DISTINCT ON (segment_id) segment_id, value, observed_at
            FROM {table_name}
            WHERE sk NOT IN (%s, %s) AND observed_at IS NOT NULL
            ORDER BY segment_id, observed_at DESC
            """,
            (AVG_SORT_KEY, SPEC_SORT_KEY),
        )
        exact_rows = cur.fetchall()

    snapshot: dict[str, dict] = {}
    for segment_id, value in avg_rows:
        snapshot.setdefault(segment_id, {})["avg"] = float(value)
    for segment_id, value in spec_rows:
        snapshot.setdefault(segment_id, {})["spec"] = float(value)
    for segment_id, value, observed_at in exact_rows:
        entry = snapshot.setdefault(segment_id, {})
        entry["exact_value"] = float(value)
        entry["exact_observed_at"] = observed_at

    return snapshot


# ==========================
# type2(길이)/type4(통행료) 공용 — 시간 무관 정적값
# ==========================
#
# type1과 달리 세그먼트당 값이 하나뿐이라(길이/통행료 모두 시간에 따라
# 안 바뀜) PK가 segment_id 하나뿐이다. 두 타입이 스키마가 완전히 같아서
# (segment_id, value, collected_date, updated_date) 함수를 공용으로 쓴다.


def ensure_static_table(table_name: str) -> None:
    """테이블이 없으면 만든다. ensure_table과 동일한 원칙(로컬 개발
    편의용, 운영 중엔 안 씀) - 스키마만 다르다."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    segment_id TEXT NOT NULL,
                    value NUMERIC NOT NULL,
                    collected_date DATE,
                    updated_date DATE,
                    PRIMARY KEY (segment_id)
                )
            """)
    except psycopg2.OperationalError:
        _reset_connection()
        raise


def batch_get_static_values(table_name: str, segment_ids: list[str]) -> dict[str, dict]:
    """segment_id 목록으로 정적값 행을 한 번에 조회한다. 없는 segment_id는
    키 자체가 없다. RDS 연결/쿼리 실패는 삼키지 않고 그대로 던진다 -
    호출부가 이걸로 "RDS 자체가 죽었다"를 판단해서 코드 상수로 넘어간다."""
    if not segment_ids:
        return {}

    unique_ids = list(dict.fromkeys(segment_ids))
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT segment_id, value, collected_date, updated_date FROM {table_name} "
                f"WHERE segment_id = ANY(%s)",
                (unique_ids,),
            )
            rows = cur.fetchall()
    except psycopg2.OperationalError:
        _reset_connection()
        raise

    return {
        segment_id: {"value": float(value), "collected_date": collected_date, "updated_date": updated_date}
        for segment_id, value, collected_date, updated_date in rows
    }


def upsert_static_items(items: list[dict], table_name: str) -> int:
    """정적값 행을 upsert한다. item은 최소 {segment_id, value}를 포함해야
    하고, collected_date/updated_date는 선택이다."""
    if not items:
        return 0

    from psycopg2.extras import execute_values

    conn = get_connection()
    rows = [
        (item["segment_id"], item["value"], item.get("collected_date"), item.get("updated_date"))
        for item in items
    ]

    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} (segment_id, value, collected_date, updated_date)
                VALUES %s
                ON CONFLICT (segment_id) DO UPDATE SET
                    value = EXCLUDED.value,
                    collected_date = EXCLUDED.collected_date,
                    updated_date = EXCLUDED.updated_date
                """,
                rows,
            )
    except psycopg2.OperationalError:
        _reset_connection()
        raise

    return len(items)
