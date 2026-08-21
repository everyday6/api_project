"""
Bronze 수집: NYC DOT Real-Time Traffic Speed Data

DOT 소스는 5분 간격으로 갱신되지만, 이 DAG는 30분마다 한 번만 폴링해서
지난 30분 범위(data_as_of 기준)를 한 번의 API 호출로 수집한다 — 5분마다
폴링하지 않는 이유는 파이프라인 전체 스케줄(설계 문서 4절, 30분 버킷)과
맞추기 위함이다. 수집된 5분 단위 판독값은 Bronze에 개별 행으로 그대로
저장한다(정제/집계는 Silver1/Gold2에서).
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.common.config import BRONZE_DIR, DATASETS
from src.common.logger import get_logger
from src.common.socrata import fetch_all, make_session

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze")

SPEED_URL = DATASETS["speed"]
BRONZE_ROOT = BRONZE_DIR / "speed"


def _soql_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _get_count(session, window_start: datetime, window_end: datetime) -> int:
    """지정 구간에 존재하는 행 수를 $select=count(*)로 가볍게 확인한다."""

    where = (
        f"data_as_of > '{_soql_timestamp(window_start)}' "
        f"AND data_as_of <= '{_soql_timestamp(window_end)}'"
    )
    response = session.get(
        SPEED_URL, params={"$select": "count(*)", "$where": where}, timeout=30
    )
    response.raise_for_status()
    return int(response.json()[0]["count"])


def has_new_speed_data(window_start: datetime, window_end: datetime) -> bool:
    """지정 구간에 새 판독값이 하나라도 있으면 True. short-circuit 태스크가 쓴다."""

    session = make_session()
    count = _get_count(session, window_start, window_end)

    logger.info(f"[speed_bronze] {window_start}~{window_end} 구간 판독값 count={count}")
    return count > 0


def collect_speed_window(
    window_start: datetime,
    window_end: datetime,
    bronze_root=BRONZE_ROOT,
) -> str:
    """지정 구간의 속도 판독값을 전부 받아 Bronze에 parquet으로 저장한다.

    결과가 0건이면 빈 문자열을 반환한다(정상 케이스 — 상위 DAG가 short-circuit
    으로 이미 걸러내지만, 이 함수 자체도 방어적으로 처리한다).
    """

    where = (
        f"data_as_of > '{_soql_timestamp(window_start)}' "
        f"AND data_as_of <= '{_soql_timestamp(window_end)}'"
    )

    rows = fetch_all(SPEED_URL, where=where, order="data_as_of")

    if not rows:
        logger.info(f"[speed_bronze] {window_start}~{window_end} 구간 결과 없음")
        return ""

    df = pd.DataFrame(rows)

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / f"window_end={window_end.strftime('%Y%m%dT%H%M')}.parquet"
    df.to_parquet(str(out_path), index=False)

    logger.info(f"[speed_bronze] {len(df)}행 저장 -> {out_path}")
    return str(out_path)
