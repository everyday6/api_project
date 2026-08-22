"""
Silver2 — LION segment x 통행료 시설/zone 매핑

toll 도메인이 자기 계산에 필요한 LION segment 정보(segment_id, street,
geometry)를 직접 뽑아 쓴다 — lion 도메인은 현재 Bronze까지만 있고
Silver1/Gold2가 없으므로(다른 브랜치에서 재구축 예정), 이 매핑에 필요한
최소한(street 이름, geometry)만 이 파일에서 직접 GDB로부터 읽는다.

매핑 이름은 조인하는 두 주체를 그대로 딴다: lion_facility(LION x 다리/터널
시설명 부분일치), lion_cbd(LION x CBD Geofence 공간조인). 둘 다 "여러
소스를 구조적으로 연결"하는 Silver2 성격이다.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

from src.common.config import SILVER2_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_silver2")

MAP_LION_FACILITY_PATH = SILVER2_DIR / "map_lion_facility.parquet"


def load_lion_segments(gdb_path: Path) -> gpd.GeoDataFrame:
    """LION Bronze GDB에서 segment_id/street/geometry만 뽑는다.

    LION 원본은 같은 segment_id가 여러 행으로 중복돼 있다(실측:
    243,237행 중 고유 segment_id는 218,373개 — 약 2.5만 건 중복). 원래
    lion/silver1.py가 이 dedup을 해줬는데 지금은 lion 도메인이 Bronze만
    있어서 이 파일에서 직접 처리한다(조용히 첫 번째 행만 남김, 기존
    lion/silver1.py와 동일한 정책)."""

    gdf = gpd.read_file(gdb_path, layer="lion")
    gdf = gdf.rename(columns={"SegmentID": "segment_id", "Street": "street"})
    gdf = gdf.drop_duplicates(subset="segment_id", keep="first")
    return gdf[["segment_id", "street", "geometry"]]


def match_lion_facilities(segments: gpd.GeoDataFrame, facilities_path: Path) -> pd.DataFrame:
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


def build_lion_facility_mapping(
    gdb_path: Path,
    facilities_path: Path = Path("config/toll_facilities.yaml"),
    out_path: Path = MAP_LION_FACILITY_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    result = match_lion_facilities(segments, facilities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] lion_facility 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)


MAP_LION_CBD_PATH = SILVER2_DIR / "map_lion_cbd.parquet"


def match_lion_cbd(segments: gpd.GeoDataFrame, zone_polygon: gpd.GeoDataFrame) -> pd.DataFrame:
    """segments 중 CBD(Congestion Relief Zone) 폴리곤과 교차하는(경계에
    걸친 것 포함) segment_id만 반환한다. intersects를 쓰는 이유: zone
    "안"으로 완전히 들어간 segment뿐 아니라 zone 경계를 지나는 진입
    segment도 혼잡통행료 대상이기 때문이다(둘을 구분할 필요 없음 — 스펙
    참고: zone 내부 segment 전부에 값을 넣고 dedup은 클라이언트가 함)."""

    if segments.crs is None:
        segments = segments.set_crs(zone_polygon.crs, allow_override=True)
    elif zone_polygon.crs is not None and segments.crs != zone_polygon.crs:
        # CBD Geofence는 위경도(EPSG:4326)로 오고 LION segment는 EPSG:2263
        # (피트)이라 좌표계가 다르면 gpd.sjoin이 경고만 내고 조용히 0건을
        # 반환한다(실제로 겪음) — 반드시 같은 좌표계로 맞춰야 한다.
        zone_polygon = zone_polygon.to_crs(segments.crs)

    joined = gpd.sjoin(segments, zone_polygon, how="inner", predicate="intersects")
    return joined[["segment_id"]].drop_duplicates().reset_index(drop=True)


def build_lion_cbd_mapping(
    gdb_path: Path,
    cbd_geofence_path: Path = Path("data/bronze/toll/cbd_geofence.geojson"),
    out_path: Path = MAP_LION_CBD_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    zone_polygon = gpd.read_file(cbd_geofence_path)

    result = match_lion_cbd(segments, zone_polygon)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] lion_cbd 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
