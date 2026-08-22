"""
Gold2 — LION dim_segment에 is_routable 붙이기

src/lion/silver1.py가 만든 dim_segment(기본 컬럼 + 원본 코드 컬럼)를 읽어서
is_routable만 계산해 붙이고 저장한다. length_ft는 이미 Silver1에 있으므로
그대로 통과시킨다 — type2(길이) 소스로 이 파일의 산출물을 그대로 쓴다.

road_class/capacity_per_hour 등은 이번 범위(nav 세그먼트 지표 API)에
필요하지 않아 계산하지 않는다(YAGNI) — 필요해지면 그때 추가한다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.config import GOLD2_DIR, SILVER1_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_gold2")

DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"
DIM_SEGMENT_PATH = GOLD2_DIR / "dim_segment.parquet"

# RW_TYPE(도로유형 코드): 차량이 통행할 수 없는 유형(Boardwalk, Path/Trail,
# Step Street, Driveway, Alley, Unknown, Non-Physical Segment, U-Turn, Ferry Route).
NON_ROUTABLE_RW_TYPES = ["5", "6", "7", "8", "10", "11", "12", "13", "14"]

VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


def _compute_is_routable(df: pd.DataFrame) -> pd.Series:
    """RW_TYPE(차량 통행 불가 유형)과 FeatureTyp(0=물리적 세그먼트)로
    차량이 실제로 지나갈 수 있는 세그먼트인지 판단한다."""

    is_non_routable = (
        df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES)
        | df["RW_TYPE"].isna()
        | (df["RW_TYPE"] == "")
    )
    is_physical = df["FeatureTyp"] == "0"

    return (~is_non_routable) & is_physical


def build_dim_segment(dim_segment_base_path: Path = DIM_SEGMENT_BASE_PATH) -> str:
    """dim_segment(Silver1)를 읽어 is_routable을 계산해 붙인 완성본을 저장한다."""

    df = pd.read_parquet(str(dim_segment_base_path))

    df["is_routable"] = _compute_is_routable(df)

    dim_segment = df[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft",
        "is_routable", "node_from", "node_to",
    ]]

    GOLD2_DIR.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(DIM_SEGMENT_PATH), index=False)

    logger.info(f"[lion_gold2] dim_segment(Gold2) {len(dim_segment)}행 저장 -> {DIM_SEGMENT_PATH}")
    return str(DIM_SEGMENT_PATH)


def validate_dim_segment(path: str) -> str:
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"

    routable_missing_geom = df.loc[df["is_routable"], "geometry"].isna()
    assert not routable_missing_geom.any(), (
        f"is_routable=True인데 geometry가 없는 행 {routable_missing_geom.sum()}개 발견"
    )

    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    logger.info(f"[lion_gold2] dim_segment(Gold2) 검증 통과 ({n}행) -> {path}")
    return path


if __name__ == "__main__":
    out = build_dim_segment()
    validate_dim_segment(out)
