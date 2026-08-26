"""
Type1(소요시간) 긴급 스펙 추정 백필 스크립트

배경: 도로 속도 원천(Socrata) API 호출 자체가 실패해 실측 데이터가 안
들어오는 동안, segment_metrics_type1에 값이 없는 (segment_id, time) 슬롯이
많다 - Fresh Exact/Historical AVG 둘 다 못 찾고 코드 상수(45초)로 응답이
떨어지는 상태다(src/serving/nav_lookup.py 참고). 원천 API가 복구될
때까지 임시로, 도로 스펙(길이 ÷ 제한속도)으로 추정한 통과시간을 avg
컬럼에 채워 넣어 최소한 세그먼트별로 그럴듯한 값을 반환하게 한다.

세그먼트당 값 하나(시간과 무관)를 48개 시간 슬롯 전부에 복제한다 - avg가
슬롯 단위 컬럼이라, 슬롯 행 자체가 없으면 그 시간에 조회했을 때 여전히
하드코딩 상수로 떨어지기 때문이다.

count는 일부러 안 채운다 - src/nav_time/gold2.py의 증분 평균 갱신 로직이
count 없는 행을 "레거시/추정치"로 보고, 실제 관측이 처음 들어오면 이
값을 버리고 새로 시작한다(new_avg = new_value, new_count = 1). 즉 원천
데이터가 복구되면 이 백필 값은 다음 정상 배치에서 자동으로 진짜
관측값으로 교체된다 - 이 스크립트가 따로 되돌릴 필요가 없다.

라이브 서빙 테이블에 바로 execute_values로 760만 행을 쓰면 Type3에서
겪었던 것과 같은 쓰기 락 경합이 재현될 수 있어(docs 5절 "실제로 검증된
사례" 참고), UNLOGGED 스테이징 테이블에 COPY로 적재한 뒤 ON CONFLICT DO
NOTHING으로 병합한다 - 이미 값이 있는 슬롯(실제 관측이든 이전 백필이든)은
절대 덮어쓰지 않는다.

S3 스냅샷은 nav_time/gold2.py의 _export_snapshot과 동일한 쿼리로 RDS
전체를 다시 읽어서 만든다(아래 _export_snapshot 참고, 원본과 로직을
맞춰뒀다 - import로 재사용하지 않는 이유는 gold2.py가 모듈 최상단에서
pyspark를 임포트해서, pyspark가 없는 환경에서는 이 스크립트 자체가 아예
실행이 안 되기 때문이다). gold_snapshot.write_snapshot은 부분 병합이
아니라 매번 RDS 전체를 다시 내보내는 계약이라(gold_snapshot.py docstring
참고), 이 스크립트가 일부만 담은 스냅샷을 만들어 쓰면 오히려 기존
스냅샷을 축소시켜버릴 위험이 있다 - 그래서 반드시 테이블 전체를 다시
읽는다.

실행 전 필요한 환경변수 (RDS 접속 - src/common/config.py가 읽음):
    RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, RDS_PASSWORD

실행:
    python scripts/backfill_type1_spec_avg.py --dry-run   # 계산 결과만 확인
    python scripts/backfill_type1_spec_avg.py             # 실제 RDS/S3에 반영
"""

from __future__ import annotations

import argparse
import io
import time
from uuid import uuid4

import pandas as pd
from psycopg2 import sql

from src.common import gold_snapshot
from src.common.config import (
    SERVING_TABLE_TYPE1,
    SERVING_TABLE_TYPE1_COLUMNS,
    SERVING_TABLE_TYPE1_KEY_COLUMNS,
    SILVER1_DIR,
)
from src.common.db import ensure_table, new_connection
from src.common.logger import get_logger

# src.lion.silver1에서 그대로 가져오지 않는 이유: 그 모듈이 최상단에서
# airflow를 임포트해서(Airflow 태스크 데코레이터용), airflow가 없는
# 환경(로컬 등)에서는 이 스크립트 자체가 임포트 단계에서 죽는다. 경로
# 조합 규칙만 동일하게 맞춰서 재사용한다(src/lion/silver1.py:
# DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet" 참고).
DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"

logger = get_logger(__name__, log_to_file=True, log_file_stem="backfill_type1_spec_avg")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0
_BUCKET_MINUTES = 30


def _time_buckets() -> list[str]:
    return [
        f"{hour:02d}{minute:02d}"
        for hour in range(24)
        for minute in range(0, 60, _BUCKET_MINUTES)
    ]


def compute_spec_estimates() -> pd.DataFrame:
    """segment_id별 스펙 기반 추정 통과시간(초)을 계산한다.

    공식은 src/nav_time/gold2.py의 SPEC 추정과 동일하다(길이÷제한속도).
    길이/제한속도가 없거나 0 이하인 세그먼트는 추정 자체가 무의미해서
    제외한다 - 그런 세그먼트는 기존처럼 코드 상수(45초)로 계속 응답한다."""

    df = pd.read_parquet(
        DIM_SEGMENT_BASE_PATH, columns=["segment_id", "length_ft", "speed_limit_mph"]
    )
    df = df[(df["length_ft"] > 0) & (df["speed_limit_mph"] > 0)].copy()
    df["avg"] = (
        (df["length_ft"] / _FEET_PER_MILE) / df["speed_limit_mph"] * _SECONDS_PER_HOUR
    ).round().astype(int)
    return df[["segment_id", "avg"]]


def build_rows(segment_avg: pd.DataFrame) -> pd.DataFrame:
    """세그먼트별 추정치를 48개 시간 슬롯 전부로 복제한다(cross join)."""

    buckets = pd.DataFrame({"time": _time_buckets()})
    return segment_avg.merge(buckets, how="cross")[["segment_id", "time", "avg"]]


def _copy_into_staging(conn, staging_table: str, rows: pd.DataFrame) -> None:
    buf = io.StringIO()
    rows.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE UNLOGGED TABLE {t} (segment_id TEXT, time TEXT, avg NUMERIC)"
            ).format(t=sql.Identifier(staging_table))
        )
        cur.copy_expert(
            sql.SQL(
                "COPY {t} (segment_id, time, avg) FROM STDIN WITH (FORMAT csv)"
            ).format(t=sql.Identifier(staging_table)).as_string(conn),
            buf,
        )


def write_to_rds(rows: pd.DataFrame) -> int:
    """스테이징 테이블에 COPY로 적재한 뒤, 라이브 테이블에 없는 슬롯만
    ON CONFLICT DO NOTHING으로 병합한다. 반환값은 실제로 새로 채워진
    행 수(이미 값이 있던 슬롯은 카운트되지 않는다)."""

    ensure_table(SERVING_TABLE_TYPE1, SERVING_TABLE_TYPE1_COLUMNS, SERVING_TABLE_TYPE1_KEY_COLUMNS)

    # 통계적으로 오래 걸리는 관리 작업이라, 서빙 경로의 1초 타임아웃을
    # 그대로 쓰면 안 된다 - 이 커넥션만 타임아웃 없이 연다.
    conn = new_connection(connect_timeout=10, statement_timeout_ms=None)
    conn.autocommit = False
    staging_table = f"tmp_type1_spec_backfill_{uuid4().hex[:8]}"
    try:
        _copy_into_staging(conn, staging_table, rows)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {live} (segment_id, time, avg) "
                    "SELECT segment_id, time, avg FROM {staging} "
                    "ON CONFLICT (segment_id, time) DO NOTHING"
                ).format(
                    live=sql.Identifier(SERVING_TABLE_TYPE1),
                    staging=sql.Identifier(staging_table),
                )
            )
            written = cur.rowcount
        conn.commit()
        return written
    except Exception:
        conn.rollback()
        raise
    finally:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier(staging_table)))
        conn.commit()
        conn.autocommit = True


def _export_snapshot(table_name: str) -> dict[str, dict[str, dict]]:
    """src/nav_time/gold2.py::_export_snapshot과 동일한 쿼리/형식(모듈
    docstring의 pyspark 의존성 회피 사유 참고). 반환값은
    segment_id -> {time: {"value","avg","last_sample_at"}}."""

    conn = new_connection(connect_timeout=10, statement_timeout_ms=None)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT segment_id, time, value, avg, last_sample_at FROM {table}").format(
                table=sql.Identifier(table_name)
            )
        )
        rows = cur.fetchall()

    snapshot: dict[str, dict[str, dict]] = {}
    for segment_id, time_slot, value, avg, last_sample_at in rows:
        entry = {}
        if value is not None and last_sample_at is not None:
            entry["value"] = float(value)
            entry["last_sample_at"] = last_sample_at.isoformat()
        if avg is not None:
            entry["avg"] = float(avg)
        if entry:
            snapshot.setdefault(segment_id, {})[time_slot] = entry
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="계산만 하고 RDS/S3에 쓰지 않음")
    args = parser.parse_args()

    started = time.monotonic()
    segment_avg = compute_spec_estimates()
    logger.info(f"[backfill_type1] 스펙 추정치 계산 완료: {len(segment_avg)}개 세그먼트")

    rows = build_rows(segment_avg)
    logger.info(f"[backfill_type1] 시간 슬롯 확장 완료: {len(rows)}행 (48슬롯 x {len(segment_avg)}세그먼트)")

    if args.dry_run:
        print(rows.head(10))
        print(f"총 {len(rows)}행 (dry-run이라 RDS/S3에 안 씀)")
        return

    written = write_to_rds(rows)
    logger.info(
        f"[backfill_type1] RDS 적재 완료: {written}행 신규 삽입 "
        f"(기존 값 있던 {len(rows) - written}행은 건너뜀)"
    )

    gold_snapshot.export_best_effort(
        "type1",
        lambda: _export_snapshot(SERVING_TABLE_TYPE1),
        logger,
        "backfill_type1_spec_avg",
    )
    logger.info("[backfill_type1] S3 스냅샷 재수출 완료")

    elapsed = time.monotonic() - started
    logger.info(f"[backfill_type1] 전체 완료: {elapsed:.1f}초")


if __name__ == "__main__":
    main()
