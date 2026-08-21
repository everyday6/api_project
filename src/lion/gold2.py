"""
Gold2 — LION dim_segment 파생 지표(road_class/capacity/is_routable)

src/lion/silver1.py가 만든 dim_segment(기본 컬럼 + 원본 코드 컬럼)를 읽어서
road_class/is_routable/is_two_way/capacity_per_hour/lane_miles를 계산해
붙이고, 같은 파일 이름(dim_segment.parquet)으로 "완성본"을 저장한다 — 여러
기존 소비처가 컬럼 위치와 무관하게 하나의 완성된 dim_segment만 알면 되도록
하기 위함이다. length_ft/capacity_per_hour 등은 nav 골드 데이터셋의
type=2(거리) 소스로도 쓰인다.

road_class / capacity 관련 숫자는 팀에서 확정한 기준이 없어 HCM(Highway Capacity
Manual) 개념을 참고한 초안이다 — 반드시 검토/조정이 필요하다 (BASE_CAPACITY_PER_LANE 참고).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.common import db
from src.common.config import GOLD2_DIR, SILVER1_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_gold2")

DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"
DIM_SEGMENT_PATH = GOLD2_DIR / "dim_segment.parquet"

# RW_TYPE(도로유형 코드, 공식 정의) -> road_class 1차 분류
HIGHWAY_RW_TYPES = ["2", "9"]                              # Highway, Ramp
NON_ROUTABLE_RW_TYPES = ["5", "6", "7", "8", "10", "11", "12", "13", "14"]
# Boardwalk, Path/Trail, Step Street, Driveway, Alley, Unknown, Non-Physical Segment, U-Turn, Ferry Route
# RW_TYPE 1(Street)/3(Bridge)/4(Tunnel)은 등급 구분이 없어서 TRUCK_ROUTE_TYPE(1=Limited
# Local, 2=Local, 3=Through)과 차로수로 arterial/local을 보조 판정한다.
ARTERIAL_TRUCK_ROUTE_TYPES = ["2", "3"]
ARTERIAL_MIN_LANES = 3

# TODO(팀 검토 필요): 확정된 사내 기준이 없어 HCM(도로용량편람) 개념 기반 초안.
# 단위: 차로당 시간당 승용차환산대수(pcphpl)
BASE_CAPACITY_PER_LANE = {
    "highway": 1900,   # HCM 자유류(freeway) 이상적 포화교통류율 근사치
    "arterial": 900,   # HCM 신호교차로 도시간선도로 차로당 용량 근사치
    "local": 600,       # 저속/주차회전 마찰이 있는 국지도로 근사치
    "non_routable": 0,
}

# TODO(팀 검토 필요): 방향계수 확정 기준 없어 1.0(보정 없음)으로 둠.
DIRECTION_FACTOR = {
    "one_way": 1.0,
    "two_way": 1.0,
}

VALID_ROAD_CLASSES = ["highway", "arterial", "local", "non_routable"]
VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


def _classify_road_class(df: pd.DataFrame) -> pd.Series:
    """RW_TYPE(+TRUCK_ROUTE_TYPE, 차로수 보조)로 road_class를 매긴다."""
    is_highway = df["RW_TYPE"].isin(HIGHWAY_RW_TYPES)
    is_non_routable = df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES) | df["RW_TYPE"].isna() | (df["RW_TYPE"] == "")
    is_arterial = (
        df["TRUCK_ROUTE_TYPE"].isin(ARTERIAL_TRUCK_ROUTE_TYPES)
        | (df["lanes_total"] >= ARTERIAL_MIN_LANES)
    )

    return pd.Series(
        np.select(
            [is_highway, is_non_routable, is_arterial],
            ["highway", "non_routable", "arterial"],
            default="local",
        ),
        index=df.index,
    )


def build_dim_segment(dim_segment_base_path: Path = DIM_SEGMENT_BASE_PATH) -> str:
    """dim_segment(Silver1)를 읽어 road_class/is_routable/is_two_way/capacity_per_hour/
    lane_miles를 계산해 붙인 완성본을 저장한다."""

    df = pd.read_parquet(str(dim_segment_base_path))

    df["road_class"] = _classify_road_class(df)
    df["is_routable"] = (df["road_class"] != "non_routable") & (df["FeatureTyp"] == "0")
    df["is_two_way"] = df["TrafDir"] == "T"

    df["base_capacity_per_lane"] = df["road_class"].map(BASE_CAPACITY_PER_LANE)
    direction_factor = np.where(df["is_two_way"], DIRECTION_FACTOR["two_way"], DIRECTION_FACTOR["one_way"])
    df["capacity_per_hour"] = df["lanes_total"] * df["base_capacity_per_lane"] * direction_factor
    df["lane_miles"] = (df["length_ft"] * df["lanes_total"]) / 5280.0

    dim_segment = df[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft", "road_class",
        "is_two_way", "lanes_total", "lane_miles", "base_capacity_per_lane",
        "capacity_per_hour", "is_routable", "node_from", "node_to",
    ]]

    GOLD2_DIR.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(DIM_SEGMENT_PATH), index=False)

    # 서빙 API가 RDS에서 읽으므로 서빙 테이블도 같이 갱신한다.
    db.write_table(dim_segment, "dim_segment")

    logger.info(f"[lion_gold2] dim_segment(Gold2) {len(dim_segment)}행 저장 -> {DIM_SEGMENT_PATH} (+ RDS)")
    return str(DIM_SEGMENT_PATH)


def validate_dim_segment(path: str) -> str:
    """
    dim_segment.parquet(Gold2 완성본)이 지켜야 할 최소한의 불변식을 확인한다.
    하나라도 깨지면 AssertionError를 던져서 태스크를 실패시킨다 — 조용히 잘못된
    데이터가 다음 단계로 넘어가는 걸 막는 게 목적이다.
    """
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"

    routable_missing_geom = df.loc[df["is_routable"], "geometry"].isna()
    assert not routable_missing_geom.any(), (
        f"is_routable=True인데 geometry가 없는 행 {routable_missing_geom.sum()}개 발견"
    )

    assert df["road_class"].isin(VALID_ROAD_CLASSES).all(), (
        f"알 수 없는 road_class 값: {sorted(set(df['road_class']) - set(VALID_ROAD_CLASSES))}"
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
