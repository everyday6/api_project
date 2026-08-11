"""
Bronze ingestion: NYC DOT Street Closures due to Construction Activities
(by Block AND Intersection 통합 버전)

Socrata dataset ID: ezy6-djsf

실제 확인된 필드 9개 (전부 PascalCase, 언더스코어 대문자 아님에 주의):
  OnStreetName, FromStreetName, ToStreetName, BoroughName,
  WorkStartDate, WorkEndDate, Purpose, OFTCode, WKT

Weekly로 갱신되는 소스. 증분 처리를 위해 WorkStartDate 기준으로
[start_date, end_date) 구간만 골라 받는다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="road_closures")

DATASET_ID = "ezy6-djsf"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
PAGE_SIZE = 50_000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

from src.common.config import BRONZE_DIR
BRONZE_ROOT = BRONZE_DIR / "road_closures"


def _fetch_all_pages(where_clause: str | None = None, select: str | None = None) -> list[dict]:
    """Socrata는 한 번에 최대 몇만 건만 주기 때문에 offset을 늘려가며 다 받는다."""
    records: list[dict] = []
    offset = 0

    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset}
        if where_clause:
            params["$where"] = where_clause
        if select:
            params["$select"] = select

        resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        records.extend(page)
        offset += PAGE_SIZE

        if len(page) < PAGE_SIZE:
            break

    return records


def check_earliest_work_start_date() -> str | None:
    """
    진단용 함수. 이 데이터셋에 실제로 WorkStartDate가 언제부터 있는지 확인한다.
    백필 돌리기 전에 한 번 실행해서 START 기준일을 정하는 데 참고.
    """
    params = {
        "$select": "WorkStartDate",
        "$order": "WorkStartDate ASC",
        "$limit": 1,
    }
    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    rows = resp.json()

    if not rows:
        logger.warning("[check_earliest] 데이터가 아예 없음")
        return None

    earliest = rows[0].get("WorkStartDate")
    logger.info(f"[check_earliest] 가장 오래된 WorkStartDate: {earliest}")
    return earliest


def ingest_road_closures(start_date: str, end_date: str, bronze_root: Path = BRONZE_ROOT) -> Path:
    """
    start_date, end_date: 'YYYY-MM-DD' 형식 문자열.
    WorkStartDate가 이 구간에 속하는 공사만 받는다 (해당 주에 새로 시작한 공사 기준).
    """

    where_clause = (
        f"WorkStartDate >= '{start_date}T00:00:00' "
        f"AND WorkStartDate < '{end_date}T00:00:00'"
    )

    records = _fetch_all_pages(where_clause=where_clause)
    df = pd.DataFrame.from_records(records)

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "nyc_dot_street_closures_by_block_and_intersection"

    dest_dir = bronze_root / f"week_start={start_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "road_closures.parquet"

    df.to_parquet(dest_path, index=False)
    logger.info(f"[road_closures] {start_date}~{end_date} 구간 {len(df)}행 저장 -> {dest_path}")
    return str(dest_path)


if __name__ == "__main__":
    # 백필 시작일을 정하기 전에, 먼저 이 데이터셋의 실제 최초 기록일을 확인
    check_earliest_work_start_date()

    # 로컬 테스트용 예시
    ingest_road_closures(start_date="2026-08-01", end_date="2026-08-08")