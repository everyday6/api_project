"""
Bronze — 공사 허가 (Street Construction Permits)

역할

- Socrata API에서 공사 허가 데이터 수집 (issuedworkstartdate 2025-01-01 이후만 —
  road_closures/stipulations와 동일하게 프로젝트에서 필요한 범위로 제한)
- API 원본을 최대한 그대로 저장
- Parquet 형식으로 날짜별 스냅샷 저장
- 동일 RUN_DATE 재실행 시 같은 경로에 저장하여 멱등성 유지

매일 이 범위 전체를 통째로 다시 받는 방식(증분 아님)이라, 이 태스크가 처음 도는
날부터 이미 2025-01-01~현재 전체가 다 받아진다 — 별도 백필 스크립트가 필요 없다
(road_closures와 동일한 이유, src/road_closures/bronze.py 참고).

※ 데이터 정제, 필터링(날짜 제외) 이외의 타입 변환 등은 수행하지 않는다.
"""

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from common.config import BRONZE_DIR, DATASETS
from common.socrata import fetch_all_streaming
from common.logger import get_logger


logger = get_logger(__name__)

SOURCE = "construction"
# permitnumber만으로는 페이지 경계에서 tie-breaker가 없어서, 같은
# permitnumber를 가진 행이 누락되거나 중복될 수 있다(construction_stipulations,
# road_closures에서 이미 겪은 문제와 동일 원인). Socrata 내부 고유 행 식별자인
# :id를 같이 걸어서 경계에서 행이 새지 않게 한다.
ORDER = "permitnumber, :id"

# 프로젝트에서 필요한 범위 (road_closures/stipulations와 동일 기준)
WHERE = "issuedworkstartdate >= '2025-01-01T00:00:00'"


def build() -> str:
    """Socrata에서 전체를 받아 저장만 한다(validate 없음) — build/validate를
    별도 Airflow 태스크로 나눠서, validate 실패로 재시도할 때 이 페이지네이션
    fetch를 처음부터 다시 안 해도 되게 하기 위함."""

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
            where=WHERE,
            order=ORDER,
            out_path=out_path,
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

    return str(out_path)


def validate_output(path: str) -> str:
    """저장된 Bronze 파일에 행이 실제로 있는지 확인한다."""
    df = pd.read_parquet(path, columns=["permitnumber"])
    if len(df) == 0:
        raise ValueError("공사 데이터를 받지 못했습니다.")
    return path


def main() -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build()
    validate_output(path)
    return path


if __name__ == "__main__":
    main()