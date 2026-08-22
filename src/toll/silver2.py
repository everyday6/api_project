"""
Silver2 — LION segment x 통행료 시설/zone 매핑

toll 도메인이 자기 계산에 필요한 LION segment 정보(segment_id, street,
geometry)를 직접 뽑아 쓴다 — lion 도메인은 현재 Bronze까지만 있고
Silver1/Gold2가 없으므로(다른 브랜치에서 재구축 예정), 이 매핑에 필요한
최소한(street 이름, geometry)만 이 파일에서 직접 GDB로부터 읽는다.

시설 매칭(다리/터널)은 street 이름 부분일치, zone 매칭(혼잡통행료 대상)은
공간조인이라 둘 다 "여러 소스를 구조적으로 연결"하는 Silver2 성격이다.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

from src.common.config import SILVER2_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_silver2")

MAP_TOLL_FACILITY_SEGMENT_PATH = SILVER2_DIR / "map_toll_facility_segment.parquet"


def load_lion_segments(gdb_path: Path) -> gpd.GeoDataFrame:
    """LION Bronze GDB에서 segment_id/street/geometry만 뽑는다."""

    gdf = gpd.read_file(gdb_path, layer="lion")
    gdf = gdf.rename(columns={"SegmentID": "segment_id", "Street": "street"})
    return gdf[["segment_id", "street", "geometry"]]


def match_toll_facilities(segments: gpd.GeoDataFrame, facilities_path: Path) -> pd.DataFrame:
    """segments의 street 컬럼이 facilities_path에 정의된 시설명 패턴을
    포함하면 그 시설로 매칭한다. 매칭 안 되는 segment는 결과에서 빠진다
    (통행료 대상 아님)."""

    facilities = yaml.safe_load(Path(facilities_path).read_text())

    rows = []
    for facility_key, rule in facilities.items():
        pattern = rule["street_contains"]
        matched = segments[segments["street"].str.contains(pattern, case=False, na=False)]
        for segment_id in matched["segment_id"]:
            rows.append({"segment_id": segment_id, "facility_key": facility_key})

    return pd.DataFrame(rows, columns=["segment_id", "facility_key"])


def build_map_toll_facility_segment(
    gdb_path: Path,
    facilities_path: Path = Path("config/toll_facilities.yaml"),
    out_path: Path = MAP_TOLL_FACILITY_SEGMENT_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    result = match_toll_facilities(segments, facilities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] 시설 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
