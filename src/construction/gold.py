"""
Gold — 공사 허가 (manhattan_construction_events)

Silver(construction_clean)는 데이터 자체의 유효성만 확인한 상태라 도시
전체 데이터를 그대로 담고 있다. 여기서는 "데이터는 멀쩡한데 우리 Traffic
Score 분석에 필요한가"를 기준으로 좁힌다.

- Manhattan만 유지
- 취소/검토 중 상태 제외
- 행정성 허가(EMBARGO) 제외
- 차량 통행과 무관한 허가 시리즈 제외

run_date 기준 "지금 활성인 공사만" 필터는 여기서도 하지 않는다 —
scoring/closure_penalty.py가 query_date/hour/요일 기준으로 이미 정확하게
판단하므로 중복 필터가 된다(construction/silver.py의 기존 코멘트와 동일한
이유).
"""

import os
from datetime import date

import pandas as pd

from src.common.config import BOROUGH, GOLD_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_gold")

SOURCE = "construction"

CONSTRUCTION_SILVER_DIR = SILVER_DIR / SOURCE

# 행정성 permit type
ADMIN_TYPES = [
    "EMBARGO",
]

# 분석 대상에서 제외할 상태
DROP_STATUS = [
    "VOIDED AFTER ISSUE",
    "SUBMITTED",
    "SIM SUBMITTED",
    "PERMIT HELD FOR EXTERNAL REVIEW",
    "FEE REVIEW",
]

# 차량 통행 영향 분석과 직접 관련이 적은 허가 시리즈
# COMMERICAL은 원본 데이터의 오타 표기
DROP_SERIES = [
    "MISCELLANEOUS CASH RECEIPTS",
    "COMMERCIAL REFUSE CONTAINER PERMIT",
    "COMMERICAL REFUSE CONTAINER PERMIT",
    "SIDEWALK CONSTRUCTION PERMIT",
    "CANOPY PERMIT",
    "VAULT LICENSE",
]


def load_silver(run_date: str) -> pd.DataFrame:
    """run_date에 해당하는 construction Silver 스냅샷을 읽는다."""

    path = CONSTRUCTION_SILVER_DIR / f"dt={run_date}" / "data.parquet"

    if not path.exists():
        raise FileNotFoundError(f"{SOURCE}: Silver 파일 없음: {path}")

    logger.info("공사 Silver 로드: path=%s", path)

    return pd.read_parquet(path)


def filter_for_traffic_score(df: pd.DataFrame) -> pd.DataFrame:
    """Traffic Score 분석에 필요한 행만 남긴다(데이터 자체는 이미 유효함)."""

    # 1. Manhattan만 유지
    before = len(df)
    df = df[df["borough"] == BOROUGH].copy()
    logger.info("맨해튼 외 자치구 제외: %d → %d", before, len(df))

    # 2. 취소 / 검토 중 상태 제외
    df = df[~df["status"].isin(DROP_STATUS)].copy()

    # 3. 행정성 허가 제외
    admin_pattern = "|".join(ADMIN_TYPES)
    df = df[
        ~df["permit_type"]
        .astype(str)
        .str.upper()
        .str.contains(admin_pattern, na=False)
    ].copy()

    # 4. 차량 통행과 직접 관련이 적은 허가 제외
    before = len(df)
    df = df[~df["permit_series"].isin(DROP_SERIES)].copy()
    logger.info("차량 무관 시리즈 제외: %d → %d", before, len(df))

    # status/borough는 이 시점 이후로 전부 동일한 값(제외 대상이 아님/Manhattan)
    # 이라 굳이 안 남긴다 — 기존 construction Silver의 최종 컬럼 구성과 동일.
    return df[[
        "permit_id",
        "permit_series",
        "permit_type",
        "linear_feet",
        "on_street",
        "from_street",
        "to_street",
        "geom_wkt",
        "work_start_ts",
        "work_end_ts",
        "permit_issue_ts",
    ]].reset_index(drop=True)


def validate(df: pd.DataFrame) -> None:
    """Gold 결과 기본 품질 검증 — Traffic Score 관점의 관련성을 확인한다."""

    if df.empty:
        raise ValueError("construction Gold 결과가 비었습니다.")

    if not df["permit_id"].is_unique:
        n_dup = int(df["permit_id"].duplicated().sum())
        raise ValueError(f"permit_id 중복 발생: {n_dup}건")

    logger.info(
        "공사 Gold 검증 완료: rows=%d",
        len(df),
    )

    logger.info(
        "permit_series 분포:\n%s",
        df["permit_series"].value_counts(dropna=False).to_string(),
    )


def build(run_date: str | None = None) -> str:
    """load -> filter -> save만 한다(validate 없음) — build/validate를 별도
    Airflow 태스크로 나눠서, validate 실패로 재시도할 때 이 변환을 다시 안
    해도 되게 하기 위함."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("공사 Gold 변환 시작: run_date=%s", run_date)

    df = load_silver(run_date)
    df = filter_for_traffic_score(df)

    path = save_parquet(df, GOLD_DIR / SOURCE / f"dt={run_date}")

    logger.info(
        "공사 Gold 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """build()가 저장한 결과를 다시 읽어 validate()를 돌린다."""
    df = pd.read_parquet(path)
    validate(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()
