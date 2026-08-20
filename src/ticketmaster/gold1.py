"""
Gold1 — TicketMaster 관련성 필터

ticketmaster_lion Silver2(전 지역, 전체 기간, LION 매핑 완료)에서 Traffic
Score에 사용할 "맨해튼 대략 범위 안이고 아직 지나지 않았으며 서비스 제외
대상이 아닌" 행사만 남긴다.

거리 계산 및 좌표계 변환은 앞 단계인 ticketmaster_lion Silver2에서 처리한다.
"""

import os
from datetime import date

import pandas as pd

from src.common.config import GOLD1_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.ticketmaster.silver1 import SOURCE

logger = get_logger(__name__, log_to_file=True, log_file_stem="ticketmaster_gold")

# 맨해튼 대략 범위
MH_LAT = (40.68, 40.88)
MH_LON = (-74.03, -73.90)

# 공연이 아니라 상시 전시/투어/유람선이라 교통 영향 성격이 다른 venue.
# 서비스 대상 선정 규칙이므로 Gold1에서 관리한다.
EXCLUDE_VENUES = {
    "BANKSY MUSEUM",
    "RADIO CITY MUSIC HALL TOUR EXPERIENCE",
    "MUSEUM OF CHINESE IN AMERICA",
    "ROCKS OFF CONCERT CRUISE SERIES",
    "THE LIBERTY BELLE - ROCKS OFF CONCERT CRUISE",
    "THE COSMO - ROCKS OFF CONCERT CRUISE",
    "CIRCLE LINE CRUISES",
    "SKYPORT MARINA",
    "JOANNE TRATTORIA",
}


def load_silver2(run_date: str) -> pd.DataFrame:
    """run_date에 해당하는 ticketmaster-LION Silver2 스냅샷을 읽는다."""
    path = SILVER2_DIR / "ticketmaster_lion" / f"dt={run_date}" / "data.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{SOURCE}: ticketmaster_lion Silver2 파일 없음 - {path}")
    return pd.read_parquet(path)


def filter_for_traffic_score(df: pd.DataFrame, run_date: str) -> pd.DataFrame:
    # 1. 맨해튼 대략 범위 필터
    df = df[
        df["lat"].between(*MH_LAT)
        & df["lon"].between(*MH_LON)
    ].copy()

    # 2. run_date 기준으로 이미 지난 행사 제외
    cutoff = date.fromisoformat(run_date)
    before = len(df)
    df = df[df["event_date"] >= cutoff].copy()
    logger.info("지난 행사 제외: %d → %d (기준 %s)", before, len(df), run_date)

    # 3. 서비스 제외 venue 판단 및 제거
    df["is_excluded"] = df["venue_name_norm"].isin(EXCLUDE_VENUES)
    before = len(df)
    df = df[~df["is_excluded"]].copy()
    logger.info("서비스 제외 venue 제거: %d → %d", before, len(df))

    # Silver2에는 추적 목적으로 먼 이벤트도 남아 있다. 실제 Traffic Score에는
    # 신뢰할 수 있는 LION segment가 연결된 이벤트만 사용한다.
    before = len(df)
    df = df[
        df["segment_id"].notna()
        & (df["mapping_method"] != "unmapped_too_far")
    ].copy()
    logger.info("LION 매핑 불가 이벤트 제외: %d → %d", before, len(df))

    return df[[
        "event_id",
        "event_date",
        "start_ts",
        "end_ts",
        "venue_name",
        "venue_name_norm",
        "lat",
        "lon",
        "segment_id",
        "distance_ft",
        "mapping_method",
    ]].reset_index(drop=True)


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("Ticketmaster Gold1 결과가 비었습니다.")

    if df.duplicated(subset=["event_id", "segment_id"]).any():
        raise ValueError("Ticketmaster (event_id, segment_id) 중복 발생")

    if df["event_date"].isna().any():
        raise ValueError("Ticketmaster event_date 결측 발생")

    if df["segment_id"].isna().any():
        raise ValueError("Ticketmaster Gold1 segment_id 결측 발생")

    logger.info("Ticketmaster Gold1 검증 완료: rows=%d", len(df))


def build(run_date: str | None = None) -> str:
    """load -> filter -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("Ticketmaster Gold1 필터 시작: run_date=%s", run_date)

    df = load_silver2(run_date)
    df = filter_for_traffic_score(df, run_date)

    path = save_parquet(df, GOLD1_DIR / SOURCE / f"dt={run_date}")

    logger.info("Ticketmaster Gold1 빌드 완료: rows=%d columns=%d path=%s", len(df), len(df.columns), path)
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
