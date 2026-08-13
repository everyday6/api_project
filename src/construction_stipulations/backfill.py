"""
Backfill: 공사 허가 스티퓰레이션 2025-01-01 ~ 현재까지 하루 단위로 채우기

bronze.py의 main()은 RUN_DATE 하루치만 받는 증분 함수라서, 과거 구간을 다
채우려면 이 함수를 매일 반복 호출해야 한다. 이 스크립트가 그 반복을 대신한다
(src/road_closures/backfill.py와 동일한 패턴).

- 이미 받아둔 날짜는 건너뛴다 (재실행해도 중복 호출 안 함 = resumable)
- API에 짧은 시간에 너무 많은 요청이 몰리지 않게 요청 사이 딜레이를 둔다
- 중간에 실패해도 나머지 날짜는 계속 진행하고, 끝나고 실패 목록을 보여준다

주의: 신규 건수 0건인 날은 bronze.py의 main()이 output 파일을 만들지 않는다
(정상 케이스로 취급하기 때문). 그래서 그런 날짜는 재실행할 때마다 매번 다시
조회하게 되는데, 이 데이터셋은 하루 평균 약 1.9만 건이라 실제로 0건인 과거
날짜는 거의 없을 것으로 보여 문제 삼지 않았다.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger
from src.construction_stipulations.bronze import SOURCE, main as ingest_one_day

logger = get_logger(__name__, log_to_file=True, log_file_stem="backfill_construction_stipulations")

# 데이터셋 자체는 2020-Present 전체를 제공하지만(Street Construction Permits -
# Stipulations (2020-Present)), road_closures와 마찬가지로 프로젝트에서 필요한
# 범위가 2025년 이후라 이 값을 그대로 유지한다 (src/road_closures/backfill.py 참고).
START = date(2025, 1, 1)
SLEEP_SECONDS = 0.3        # 요청 사이 최소 대기 시간 (rate limit 방지)


def _date_range(start: date, end: date):
    """start부터 end까지(끝 포함) 하루 단위로 날짜를 만들어 낸다."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _already_ingested(run_date: date) -> bool:
    """이미 해당 날짜 파일이 있으면 다시 안 받는다 (재실행 안전성)."""
    path = BRONZE_DIR / SOURCE / f"dt={run_date}" / "data.parquet"
    return path.exists()


def backfill_construction_stipulations(start: date = START, end: date | None = None):
    if end is None:
        # 오늘은 소스 반영이 아직 안 끝났을 수 있어 어제까지만 백필한다
        # (오늘치는 평소처럼 ingest_daily의 일일 증분 태스크가 알아서 받는다).
        end = date.today() - timedelta(days=1)

    failed_dates: list[str] = []
    total = 0
    skipped = 0
    ingested = 0

    for run_date in _date_range(start, end):
        total += 1

        if _already_ingested(run_date):
            skipped += 1
            logger.info(f"[skip] {run_date} 이미 존재, 건너뜀")
            continue

        try:
            os.environ["RUN_DATE"] = str(run_date)
            ingest_one_day()
            ingested += 1
            logger.info(f"[ok] {run_date} 적재 완료")
        except Exception as e:
            logger.error(f"[fail] {run_date} 실패: {e}")
            failed_dates.append(str(run_date))
        finally:
            time.sleep(SLEEP_SECONDS)

    logger.info("--- 백필 완료 ---")
    logger.info(f"전체 날짜: {total}, 신규 적재: {ingested}, 건너뜀: {skipped}, 실패: {len(failed_dates)}")
    if failed_dates:
        logger.warning("실패한 날짜 (재실행 시 자동으로 이 날짜만 다시 시도됨):")
        for d in failed_dates:
            logger.warning(f"  - {d}")


if __name__ == "__main__":
    backfill_construction_stipulations()
