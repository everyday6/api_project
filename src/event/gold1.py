"""
Gold1 — NYC 행사 관련성 필터

Event-LION Silver2(전 자치구, 전체 기간, 도로 매핑 완료)에서 Traffic
Score에 사용할 "차량 통행에 영향을 주는, 아직 끝나지 않은 Manhattan
행사"만 남긴다.

필터 순서
1. 맨해튼
2. run_date 기준 종료된 행사 제외
3. 보도만 막는 유형/도로 통제 없는 유형 제외 (차량 무관)

closure_type별 가중치는 Gold2(교차도메인, src/gold2/event_boost.py)에서 계산한다.
"""

import os
from datetime import date

import pandas as pd

from src.common.config import BOROUGH_EVENT, GOLD1_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.event.silver1 import SOURCE

logger = get_logger(__name__, log_to_file=True, log_file_stem="event_gold")

# 보도만 막아 차량 통행에는 영향이 없는 유형
SIDEWALK_ONLY = [
    "Partial Sidewalk Closure",
    "Full Sidewalk Closure",
]


def load_silver2(run_date: str) -> pd.DataFrame:
    """run_date에 해당하는 Event-LION Silver2 스냅샷을 읽는다."""
    path = SILVER2_DIR / "event_lion" / f"dt={run_date}" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{SOURCE}: Event-LION Silver2 파일 없음 - {path}")
    return pd.read_parquet(str(path))


def filter_for_traffic_score(df: pd.DataFrame, run_date: str) -> pd.DataFrame:
    # 1. 맨해튼만
    df = df[df["event_borough"] == BOROUGH_EVENT].copy()

    # 2. run_date 기준으로 이미 끝난 행사 제외
    cutoff = pd.Timestamp(run_date)
    before = len(df)
    df = df[df["end_ts"] >= cutoff].copy()
    logger.info("종료 행사 제외: %d → %d (기준 %s)", before, len(df), run_date)

    # 3. 차량 통행에 영향 있는 도로 통제만 유지
    before = len(df)
    df = df[
        df["closure_type"].ne("N/A")
        & ~df["closure_type"].isin(SIDEWALK_ONLY)
    ].copy()
    logger.info("차량 무관 행사 제외: %d → %d", before, len(df))

    return df[[
        "event_id",
        "start_ts",
        "end_ts",
        "closure_type",
        "on_street",
        "from_street",
        "to_street",
        "segment_id",
        "is_routable",
        "mapping_status",
        "unmatched_reason",
    ]].reset_index(drop=True)


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("event Gold1 결과가 비었습니다.")

    if df["event_id"].isna().any():
        raise ValueError("event_id NULL 발생")

    if df.duplicated(subset=["event_id", "start_ts", "segment_id"]).any():
        raise ValueError("(event_id, start_ts, segment_id) 중복 발생")

    logger.info(
        "행사 Gold1 검증 완료: rows=%d events=%d",
        len(df), df["event_id"].nunique(),
    )
    logger.info("closure_type 분포:\n%s", df["closure_type"].value_counts().to_string())


def build(run_date: str | None = None) -> str:
    """load -> filter -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("행사 Gold1 필터 시작: run_date=%s", run_date)

    df = load_silver2(run_date)
    df = filter_for_traffic_score(df, run_date)

    path = save_parquet(df, GOLD1_DIR / SOURCE / f"dt={run_date}")

    logger.info("행사 Gold1 빌드 완료: rows=%d columns=%d path=%s", len(df), len(df.columns), path)
    return str(path)


def validate_output(path: str) -> str:
    """build()가 저장한 결과를 다시 읽어 validate()를 돌린다."""
    df = pd.read_parquet(str(path))
    validate(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()
