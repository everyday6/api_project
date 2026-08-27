"""
속도 링크(LineString) <-> LION segment 공간 매칭

src/silver2/ticketmaster_lion.py의 venue(Point)-LION 매핑과 동일한
buffer+intersects+nearest-fallback 패턴을 쓴다 — 대상이 LineString이라는
점만 다르다. distinct link 개수(수천 규모)에 대해서만 계산하도록 상위
호출부(src/silver2/segment_speed.py)가 이 함수를 호출한다.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import LineString

from src.common.config import (
    LION_CRS,
    SPEED_CRS,
    SPEED_LION_BUFFER_FT,
    SPEED_LION_MAX_DISTANCE_FT,
    SPEED_LION_WARN_DISTANCE_FT,
)
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="segment_speed_match")


def parse_link_points(link_points: str) -> LineString | None:
    """'lat1,lon1 lat2,lon2 ...' 형식을 LineString(lon, lat 순서)으로 바꾼다.

    점이 2개 미만이면 선을 만들 수 없으므로 None을 반환한다.
    """
    coords = []
    for pair in link_points.strip().split():
        try:
            lat_str, lon_str = pair.split(",")
            coords.append((float(lon_str), float(lat_str)))
        except ValueError:
            continue

    if len(coords) < 2:
        return None

    return LineString(coords)


def _build_link_gdf(links_df: pd.DataFrame) -> gpd.GeoDataFrame:
    work = links_df.copy()
    work["geometry"] = work["link_points"].apply(parse_link_points)
    work = work[work["geometry"].notna()]

    if work.empty:
        return gpd.GeoDataFrame(columns=["link_id", "geometry"], geometry="geometry", crs=LION_CRS)

    gdf = gpd.GeoDataFrame(work, geometry="geometry", crs=SPEED_CRS)
    return gdf.to_crs(LION_CRS)


def _build_lion_gdf(dim_segment_df: pd.DataFrame) -> gpd.GeoDataFrame:
    work = dim_segment_df[dim_segment_df["geometry"].notna()].copy()
    work["geometry"] = work["geometry"].apply(wkt.loads)
    return gpd.GeoDataFrame(work, geometry="geometry", crs=LION_CRS)


def match_links_to_segments(links_df: pd.DataFrame, dim_segment_df: pd.DataFrame) -> pd.DataFrame:
    """속도 링크를 LION segment에 매핑한다.

    1. 링크 buffer(SPEED_LION_BUFFER_FT) 안에 겹치는 모든 segment를 찾는다
       (링크 하나가 여러 블록 segment에 걸치는 경우를 반영).
    2. buffer 안에 아무 segment도 없는 링크는 nearest 1개로 fallback한다.
    3. nearest도 SPEED_LION_MAX_DISTANCE_FT보다 멀면 매핑하지 않는다.
    """

    link_gdf = _build_link_gdf(links_df)
    if link_gdf.empty:
        return pd.DataFrame(columns=["link_id", "segment_id", "distance_ft", "mapping_method"])

    lion_gdf = _build_lion_gdf(dim_segment_df)

    buffer_gdf = link_gdf.copy()
    buffer_gdf["geometry"] = buffer_gdf.geometry.buffer(SPEED_LION_BUFFER_FT)

    joined = gpd.sjoin(
        buffer_gdf,
        lion_gdf[["segment_id", "geometry"]],
        how="left",
        predicate="intersects",
    )

    lion_geometry = lion_gdf.set_index("segment_id").geometry

    def _distance(row):
        if pd.isna(row["segment_id"]):
            return pd.NA
        link_geom = link_gdf.loc[row.name, "geometry"]
        return link_geom.distance(lion_geometry.loc[row["segment_id"]])

    joined["distance_ft"] = joined.apply(_distance, axis=1)

    matched_link_ids = set(joined.loc[joined["segment_id"].notna(), "link_id"])
    fallback_gdf = link_gdf[~link_gdf["link_id"].isin(matched_link_ids)].copy()

    nearest = None
    if not fallback_gdf.empty:
        nearest = gpd.sjoin_nearest(
            fallback_gdf,
            lion_gdf[["segment_id", "geometry"]],
            how="left",
            distance_col="distance_ft",
        )
        nearest["mapping_method"] = "nearest_fallback"

        over_warn = nearest["distance_ft"] > SPEED_LION_WARN_DISTANCE_FT
        if over_warn.any():
            logger.warning(
                f"nearest fallback 경고 거리 {SPEED_LION_WARN_DISTANCE_FT}ft 초과: {int(over_warn.sum())}건"
            )

        too_far = nearest["distance_ft"] > SPEED_LION_MAX_DISTANCE_FT
        if too_far.any():
            logger.warning(f"nearest fallback 최대 거리 초과: {int(too_far.sum())}건 매핑 제외")
            nearest = nearest[~too_far]

    buffer_result = joined[joined["segment_id"].notna()].copy()
    buffer_result["mapping_method"] = "buffer"

    result_columns = ["link_id", "segment_id", "distance_ft", "mapping_method"]
    parts = [buffer_result[result_columns]]
    if nearest is not None and not nearest.empty:
        parts.append(nearest[result_columns])

    result = pd.concat(parts, ignore_index=True)
    result = result.drop_duplicates(subset=["link_id", "segment_id"], keep="first")

    logger.info(f"link-segment 매칭 완료: links={len(link_gdf)} rows={len(result)}")

    return result
