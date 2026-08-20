"""
Silver2 — construction(공사 허가) x construction_work_hours_rules(작업 시간대
제약, construction_stipulations Silver1의 산출물)를 LEFT JOIN해 permit마다
work_hours 규칙을 붙인 construction_work_hours를 만든다.

Silver2는 항상 상위 도메인의 Silver1을 읽는다는 원칙에 따라 construction
**Gold1**이 아니라 construction **Silver1**(전 지역, 필터링 전)을 읽는다 —
이렇게 해야 이후 Gold1 단계(지역/상태 필터)가 이 조인 결과에도 그대로
적용될 수 있다. 예전 버전은 construction Gold(이미 Manhattan 등으로 필터링된
상태)를 읽었다.
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd

from src.common.config import SILVER1_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.silver1 import load_built_work_hours_rules

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_stipulations_silver")

OUT_SOURCE = "construction_work_hours"

CONSTRUCTION_SILVER1_DIR = SILVER1_DIR / "construction"


def load_construction_silver1(run_date: str) -> pd.DataFrame:
    """run_date에 해당하는 construction Silver1 스냅샷을 읽는다."""
    path = CONSTRUCTION_SILVER1_DIR / f"dt={run_date}" / "data.parquet"
    return pd.read_parquet(path)


def _merge_work_hours(construction: pd.DataFrame) -> pd.DataFrame:
    work_hours = load_built_work_hours_rules()

    return construction.merge(
        work_hours,
        left_on="permit_id",
        right_on="permitnumber",
        how="left",
    ).drop(columns=["permitnumber"])


def validate(df: pd.DataFrame, construction_rows: int) -> None:
    if df.empty:
        raise ValueError("construction_work_hours Silver2 결과가 비었습니다.")

    if df["permit_id"].isna().any():
        raise ValueError("permit_id NULL 발생")

    # LEFT JOIN이라 원본(construction) 행수보다 적을 수 없다 (여러 시간대 규칙이면 더 늘어남).
    if len(df) < construction_rows:
        raise ValueError(
            f"조인 후 행수({len(df)})가 원본 construction 행수({construction_rows})보다 적음 — LEFT JOIN 오류 가능성"
        )

    has_rule = df["work_start_hour"].notna().sum()
    logger.info(
        "construction_work_hours Silver2 검증 완료: rows=%d (원본 construction=%d), 시간대 제약 있는 행=%d (%.1f%%)",
        len(df), construction_rows, has_rule, has_rule / len(df) * 100,
    )
    logger.info("work_days_code 분포:\n%s", df["work_days_code"].value_counts(dropna=False).to_string())


def build(run_date: str | None = None) -> str:
    """load -> merge -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("construction_work_hours Silver2 조인 시작: run_date=%s", run_date)

    construction = load_construction_silver1(run_date)
    df = _merge_work_hours(construction)

    path = save_parquet(df, SILVER2_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "construction_work_hours Silver2 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


def validate_output(path: str, run_date: str) -> str:
    """build()가 저장한 결과를 다시 읽어, 그 run_date의 construction Silver1
    행수와 비교하며 validate()를 돌린다."""
    df = pd.read_parquet(path)
    construction_rows = len(load_construction_silver1(run_date))
    validate(df, construction_rows=construction_rows)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())
    path = build(run_date)
    validate_output(path, run_date)
    return path


if __name__ == "__main__":
    main()
