"""
Bronze — 공사 허가 (Street Construction Permits)

역할

- Socrata API에서 공사 허가 전체 데이터 수집
- API 원본을 최대한 그대로 저장
- Parquet 형식으로 날짜별 스냅샷 저장
- 동일 RUN_DATE 재실행 시 같은 경로에 저장하여 멱등성 유지

※ 증분 수집, 데이터 정제, 필터링, 타입 변환 등은 수행하지 않는다.
"""

import sys
import os
from pathlib import Path
from datetime import date

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from common.config import BRONZE_DIR, DATASETS
from common.socrata import fetch_all_streaming
from common.logger import get_logger


logger = get_logger(__name__)

SOURCE = "construction"
ORDER = "permitnumber"


def main():

    run_date = os.getenv(
        "RUN_DATE",
        date.today().isoformat(),
    )

    out_path = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )

    logger.info(
        "공사 전체 수집 시작: run_date=%s",
        run_date,
    )

    try:
        total = fetch_all_streaming(
            url=DATASETS[SOURCE],
            where="1=1",
            order=ORDER,
            out_path=out_path,
        )

        if total == 0:
            raise ValueError(
                "공사 데이터를 받지 못했습니다."
            )

        logger.info(
            "공사 전체 수집 완료: rows=%d path=%s",
            total,
            out_path,
        )

    except Exception:
        logger.exception(
            "공사 전체 수집 실패: run_date=%s",
            run_date,
        )
        raise


if __name__ == "__main__":
    main()