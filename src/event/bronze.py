import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, DATASETS
from common.socrata import fetch_all
from common.utils import save_parquet
from common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="event_bronze")

SOURCE = "event"
ORDER = "event_id"


def build(run_date: str | None = None) -> str:
    """fetch -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    out_dir = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
    )

    logger.info(
        "행사 수집 시작: run_date=%s",
        run_date,
    )

    rows = fetch_all(
        url=DATASETS[SOURCE],
        where="1=1",
        order=ORDER,
    )

    df = pd.DataFrame(rows)

    path = save_parquet(
        df,
        out_dir,
    )

    logger.info(
        "행사 수집 빌드 완료: rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """저장된 Bronze 파일에 행이 실제로 있는지 확인한다."""
    df = pd.read_parquet(path)
    if df.empty:
        raise ValueError(f"{SOURCE}: 받은 데이터가 없음")
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()