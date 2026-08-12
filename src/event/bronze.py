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

logger = get_logger(__name__)

SOURCE = "event"
ORDER = "event_id"


def main(run_date: str | None = None):
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

    if df.empty:
        raise ValueError(
            f"{SOURCE}: 받은 데이터가 없음"
        )

    path = save_parquet(
        df,
        out_dir,
    )

    logger.info(
        "행사 수집 완료: rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )


if __name__ == "__main__":
    main()