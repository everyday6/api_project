"""
Backfill: 도로 통제(road_closures) 2025-01-01 ~ 현재까지 주 단위로 채우기

road_closures.py의 ingest_road_closures(start_date, end_date)는
"이번 주" 하나만 받는 증분 함수라서, 과거 80주치를 다 채우려면
이 함수를 매 주차마다 반복 호출해야 한다. 이 스크립트가 그 반복을 대신한다.

- 이미 받아둔 주차는 건너뛴다 (재실행해도 중복 호출 안 함 = resumable)
- API에 짧은 시간에 너무 많은 요청이 몰리지 않게 요청 사이 딜레이를 둔다
- 중간에 실패해도 나머지 주차는 계속 진행하고, 끝나고 실패 목록을 보여준다
"""

from __future__ import annotations

import time
from datetime import date, timedelta
from pathlib import Path

from src.common.logger import get_logger
from src.road_closures.bronze import ingest_road_closures, BRONZE_ROOT

logger = get_logger(__name__, log_to_file=True, log_file_stem="backfill_road_closures")

START = date(2025, 1, 1)   # 백필 시작점
SLEEP_SECONDS = 1.0        # 요청 사이 최소 대기 시간 (rate limit 방지)


def _week_ranges(start: date, end: date):
    """start부터 end까지 7일 단위로 (주 시작일, 주 종료일) 쌍을 만들어 낸다."""
    current = start
    while current < end:
        next_week = current + timedelta(days=7)
        yield current, min(next_week, end)
        current = next_week


def _already_ingested(week_start: date, bronze_root: Path = BRONZE_ROOT) -> bool:
    """이미 해당 주차 파일이 있으면 다시 안 받는다 (재실행 안전성)."""
    path = bronze_root / f"week_start={week_start}" / "road_closures.parquet"
    return path.exists()


def backfill_road_closures(start: date = START, end: date | None = None):
    if end is None:
        end = date.today()

    failed_weeks: list[str] = []
    total = 0
    skipped = 0

    for week_start, week_end in _week_ranges(start, end):
        total += 1

        if _already_ingested(week_start):
            skipped += 1
            logger.info(f"[skip] {week_start} 이미 존재, 건너뜀")
            continue

        try:
            ingest_road_closures(start_date=str(week_start), end_date=str(week_end))
            logger.info(f"[ok] {week_start}~{week_end} 적재 완료")
        except Exception as e:
            logger.error(f"[fail] {week_start}~{week_end} 실패: {e}")
            failed_weeks.append(str(week_start))
        finally:
            time.sleep(SLEEP_SECONDS)

    logger.info("--- 백필 완료 ---")
    logger.info(f"전체 주차: {total}, 건너뜀: {skipped}, 실패: {len(failed_weeks)}")
    if failed_weeks:
        logger.warning("실패한 주차 (재실행 시 자동으로 이 주차만 다시 시도됨):")
        for w in failed_weeks:
            logger.warning(f"  - {w}")


if __name__ == "__main__":
    backfill_road_closures()
