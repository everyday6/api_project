"""
Type1(소요시간) 긴급 스펙 추정 백필 스크립트 (청크 버전)

배경: 도로 속도 원천(Socrata) API 호출 자체가 실패해 실측 데이터가 안
들어오는 동안, segment_metrics_type1에 값이 없는 (segment_id, time) 슬롯이
많다 - Fresh Exact/Historical AVG 둘 다 못 찾고 코드 상수(45초)로 응답이
떨어지는 상태다(src/serving/nav_lookup.py 참고). 원천 API가 복구될
때까지 임시로, 도로 스펙(길이 ÷ 제한속도)으로 추정한 통과시간을 avg
컬럼에 채워 넣어 최소한 세그먼트별로 그럴듯한 값을 반환하게 한다.

세그먼트당 값 하나(시간과 무관)를 48개 시간 슬롯 전부에 복제한다 - avg가
슬롯 단위 컬럼이라, 슬롯 행 자체가 없으면 그 시간에 조회했을 때 여전히
하드코딩 상수로 떨어지기 때문이다.

value 컬럼도 NOT NULL이라 avg와 같은 추정치를 넣어야 새 행이 들어간다.
다만 last_sample_at은 일부러 비워둔다 - _is_fresh(last_sample_at)가
None이면 무조건 False를 반환해서(src/serving/nav_lookup.py 참고),
value가 채워져 있어도 "Fresh Exact"로는 절대 채택되지 않고 그대로
avg 단계로 내려간다.

count는 일부러 안 채운다 - src/nav_time/gold2.py의 증분 평균 갱신 로직이
count 없는 행을 "레거시/추정치"로 보고, 실제 관측이 처음 들어오면 이
값을 버리고 새로 시작한다(new_avg = new_value, new_count = 1). 원천
데이터가 복구되면 이 백필 값은 다음 정상 배치에서 자동으로 진짜
관측값으로 교체된다.

--- 청크로 나눈 이유 ---
첫 시도는 16만 세그먼트 x 48슬롯 = 650만 행을 pandas DataFrame + CSV
버퍼로 한 번에 메모리에 올렸다가, 작은 인스턴스에서 메모리 압박으로
EC2 전체가 응답 불능(SSH까지 막힘)이 됐다. 이번엔 세그먼트를
--chunk-size(기본 1,000개)씩 잘라서, 청크 하나(1,000세그먼트 x 48슬롯
= 48,000행)만 메모리에 올리고 바로 RDS에 반영한 뒤 버린다 - 피크
메모리 사용량이 세그먼트 총량과 무관하게 항상 청크 크기로 고정된다.

각 청크는 독립 트랜잭션(커밋 단위)이라, 중간에 죽어도 이미 처리된
청크는 안전하게 남는다. 진행 상황은 로컬 체크포인트 파일
(/tmp/backfill_type1_checkpoint.txt)에 "마지막으로 끝낸 청크 번호"를
기록해서, 재실행하면 처음부터 다시 하지 않고 이어서 진행한다(ON
CONFLICT DO NOTHING이라 이미 끝난 청크를 다시 밀어넣어도 안전하지만,
굳이 다시 계산/전송할 필요가 없어서 체크포인트로 건너뛴다).

라이브 서빙 테이블에 execute_values로 바로 쓰면 Type3에서 겪었던 것과
같은 쓰기 락 경합이 재현될 수 있어(docs 5절 "실제로 검증된 사례"
참고), 청크마다 작은 UNLOGGED 스테이징 테이블에 COPY로 적재한 뒤 ON
CONFLICT DO NOTHING으로 병합한다.

S3 스냅샷은 nav_time/gold2.py의 _export_snapshot과 동일한 쿼리로 RDS
전체를 다시 읽어서 만든다(import로 재사용하지 않는 이유는 gold2.py가
모듈 최상단에서 pyspark를 임포트해서, pyspark가 없는 환경에서는 이
스크립트 자체가 아예 실행이 안 되기 때문). AWS 자격증명이 없어 실패해도
export_best_effort가 예외를 삼키고 로그만 남기므로 전체 스크립트는
정상 종료된다 - RDS 백필(진짜 문제 해결)과 무관한 부가 단계다.

실행 전 필요한 환경변수 (RDS 접속 - src/common/config.py가 읽음):
    RDS_HOST, RDS_PORT, RDS_DB, RDS_USER, RDS_PASSWORD

실행:
    # 계산 결과만 확인 (RDS/S3 안 건드림)
    python scripts/backfill_type1_spec_avg.py --dry-run

    # 실제 반영 - 청크 크기/청크 사이 대기시간 조절 가능
    python scripts/backfill_type1_spec_avg.py
    python scripts/backfill_type1_spec_avg.py --chunk-size 500 --sleep 1.0

    # 중간에 죽었다가 재실행 - 체크포인트부터 자동으로 이어서 진행
    python scripts/backfill_type1_spec_avg.py

    # 체크포인트 무시하고 처음부터 다시
    python scripts/backfill_type1_spec_avg.py --restart
"""

from __future__ import annotations

import argparse
import io
import time
from pathlib import Path
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
# 조합 규칙만 동일하게 재사용한다(src/lion/silver1.py:
# DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet" 참고).
DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"

CHECKPOINT_PATH = Path("/tmp/backfill_type1_checkpoint.txt")

logger = get_logger(__name__, log_to_file=True, log_file_stem="backfill_type1_spec_avg")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0
_BUCKET_MINUTES = 30


def _log(message: str) -> None:
    """logger(파일)와 stdout(터미널에서 바로 보이게) 둘 다에 즉시 남긴다.
    print를 flush=True로 강제하는 이유: 표준출력이 파이프/리다이렉트로
    버퍼링되면 "화면에 아무것도 안 찍혀서 죽은 줄 알았다"는 상황이
    재현될 수 있어서다."""

    logger.info(message)
    print(message, flush=True)


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
    return df[["segment_id", "avg"]].reset_index(drop=True)


def build_chunk_rows(segment_avg_chunk: pd.DataFrame, buckets: pd.DataFrame) -> pd.DataFrame:
    """세그먼트 청크 하나를 48개 시간 슬롯으로 복제한다(cross join).

    value는 avg와 같은 값을 넣는다 - NOT NULL 제약 때문일 뿐, last_sample_at을
    비워두므로 Fresh Exact로 채택되지는 않는다(위 모듈 docstring 참고)."""

    rows = segment_avg_chunk.merge(buckets, how="cross")
    rows["value"] = rows["avg"]
    return rows[["segment_id", "time", "value", "avg"]]


def _copy_into_staging(conn, staging_table: str, rows: pd.DataFrame) -> None:
    buf = io.StringIO()
    rows.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE UNLOGGED TABLE {t} (segment_id TEXT, time TEXT, value NUMERIC, avg NUMERIC)"
            ).format(t=sql.Identifier(staging_table))
        )
        cur.copy_expert(
            sql.SQL(
                "COPY {t} (segment_id, time, value, avg) FROM STDIN WITH (FORMAT csv)"
            ).format(t=sql.Identifier(staging_table)).as_string(conn),
            buf,
        )


def write_chunk(conn, rows: pd.DataFrame) -> int:
    """청크 하나를 스테이징 테이블에 COPY로 적재한 뒤, 라이브 테이블에
    없는 슬롯만 ON CONFLICT DO NOTHING으로 병합하고 커밋한다. 반환값은
    실제로 새로 채워진 행 수."""

    staging_table = f"tmp_type1_spec_{uuid4().hex[:8]}"
    try:
        _copy_into_staging(conn, staging_table, rows)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {live} (segment_id, time, value, avg) "
                    "SELECT segment_id, time, value, avg FROM {staging} "
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


def _load_checkpoint() -> int:
    """마지막으로 끝낸 청크 번호 + 1(다음에 시작할 청크 번호)을 반환한다.
    체크포인트 파일이 없거나 형식이 이상하면 0부터(처음부터) 시작한다."""

    if not CHECKPOINT_PATH.exists():
        return 0
    try:
        return int(CHECKPOINT_PATH.read_text().strip()) + 1
    except ValueError:
        return 0


def _save_checkpoint(chunk_index: int) -> None:
    CHECKPOINT_PATH.write_text(str(chunk_index))


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
    parser.add_argument("--chunk-size", type=int, default=1000, help="한 번에 처리할 세그먼트 수(기본 1000)")
    parser.add_argument("--sleep", type=float, default=0.3, help="청크 사이 대기 시간(초, 기본 0.3)")
    parser.add_argument("--restart", action="store_true", help="체크포인트 무시하고 처음부터 다시 시작")
    args = parser.parse_args()

    started = time.monotonic()
    segment_avg = compute_spec_estimates()
    total_segments = len(segment_avg)
    _log(f"[backfill_type1] 스펙 추정치 계산 완료: {total_segments}개 세그먼트")

    buckets = pd.DataFrame({"time": _time_buckets()})
    total_chunks = (total_segments + args.chunk_size - 1) // args.chunk_size
    _log(
        f"[backfill_type1] 청크 크기={args.chunk_size}세그먼트, 총 {total_chunks}개 청크, "
        f"청크당 최대 {args.chunk_size * len(buckets)}행"
    )

    if args.dry_run:
        sample = build_chunk_rows(segment_avg.iloc[:5], buckets)
        print(sample.head(10))
        print(f"총 예상 {total_segments * len(buckets)}행 (dry-run이라 RDS/S3에 안 씀)")
        return

    if args.restart and CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
        _log("[backfill_type1] --restart: 체크포인트 삭제, 처음부터 시작")

    start_chunk = _load_checkpoint()
    if start_chunk > 0:
        _log(f"[backfill_type1] 체크포인트 발견: 청크 {start_chunk}번부터 이어서 진행")

    ensure_table(SERVING_TABLE_TYPE1, SERVING_TABLE_TYPE1_COLUMNS, SERVING_TABLE_TYPE1_KEY_COLUMNS)
    conn = new_connection(connect_timeout=10, statement_timeout_ms=None)
    conn.autocommit = False

    total_written = 0
    chunk_started = time.monotonic()
    try:
        for chunk_index in range(start_chunk, total_chunks):
            offset = chunk_index * args.chunk_size
            chunk = segment_avg.iloc[offset : offset + args.chunk_size]
            rows = build_chunk_rows(chunk, buckets)

            written = write_chunk(conn, rows)
            total_written += written
            _save_checkpoint(chunk_index)

            done = chunk_index - start_chunk + 1
            remaining = total_chunks - chunk_index - 1
            elapsed = time.monotonic() - chunk_started
            avg_per_chunk = elapsed / done
            eta_min = (avg_per_chunk * remaining) / 60
            _log(
                f"[backfill_type1] 청크 {chunk_index + 1}/{total_chunks} 완료 "
                f"({len(rows)}행 처리, {written}행 신규) - 누적 {total_written}행, "
                f"진행률 {(chunk_index + 1) / total_chunks * 100:.1f}%, 예상 잔여 {eta_min:.1f}분"
            )

            if args.sleep > 0 and chunk_index < total_chunks - 1:
                time.sleep(args.sleep)
    finally:
        conn.autocommit = True
        conn.close()

    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()
    _log(f"[backfill_type1] RDS 적재 전체 완료: 총 {total_written}행 신규 삽입")

    gold_snapshot.export_best_effort(
        "type1",
        lambda: _export_snapshot(SERVING_TABLE_TYPE1),
        logger,
        "backfill_type1_spec_avg",
    )
    _log("[backfill_type1] S3 스냅샷 재수출 완료(또는 best-effort 실패 - 위 로그 참고)")

    elapsed = time.monotonic() - started
    _log(f"[backfill_type1] 전체 완료: {elapsed / 60:.1f}분")


if __name__ == "__main__":
    main()
