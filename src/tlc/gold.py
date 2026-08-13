"""
TLC Gold — 세그먼트x평일시간대 통행량

TLC 하차(dropoff) 데이터를 "택시 수요"가 아니라 "일반적인 도로 교통량 프록시"로
간주하고, 세그먼트별로 평일 0~23시 각 시간대에 상대적으로 얼마나 붐비는지를
나타내는 Gold 테이블(dim_segment_tlc_volume)을 만든다.

자세한 배경은 docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md
참고.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from src.common.config import SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")

DIM_SEGMENT_TLC_VOLUME_PATH = SILVER_DIR / "dim_segment_tlc_volume.parquet"

HOURS = list(range(24))
DEFAULT_HOPS = 3


def _expand_zone_to_segment_hour(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """zone x hour 하차수를 segment x hour로 펼친다.

    같은 zone에 속한 세그먼트는 zone 총합을 그대로 나눠 갖지 않고 동일하게
    받는다(세그먼트 수로 나누지 않음). 매치 안 된 시간대는 0으로 채워서
    세그먼트마다 정확히 24행을 보장한다.
    """

    segment_zone = map_zone_segment[["segment_id", "zone_id"]].copy()
    segment_zone["zone_id"] = segment_zone["zone_id"].astype("int64")

    hours = pd.DataFrame({"hour": HOURS})

    grid = segment_zone.merge(hours, how="cross")

    counts = zone_hour_counts.copy()
    counts["zone_id"] = counts["zone_id"].astype("int64")
    counts["hour"] = counts["hour"].astype("int64")

    merged = grid.merge(counts, on=["zone_id", "hour"], how="left")
    merged["dropoff_count_raw"] = merged["dropoff_count"].fillna(0).astype("int64")

    return merged[["segment_id", "hour", "dropoff_count_raw"]]


def _normalize_tlc_volume(df: pd.DataFrame) -> pd.DataFrame:
    """dropoff_count_raw를 전체 (segment_id, hour) 조합 기준 global percentile
    rank(0~1)로 정규화한다. dim_segment_traffic_score_v0의 demand_raw(중심성)를
    만들 때 쓴 방식과 동일하다 — 세그먼트/시간대별로 따로 rank하지 않고 전부
    하나로 묶어서 비교한다.
    """

    result = df.copy()
    result["tlc_volume"] = result["dropoff_count_raw"].rank(pct=True, method="average")
    return result
