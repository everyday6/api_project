"""합성 속도 데이터 생성 — 실시간 속도 피드가 커버 안 하는 LION 세그먼트용.

실제 속도 피드는 고정된 125개 link뿐이라 LION routable 세그먼트의 약
7.6%만 커버한다(나머지 92%는 매칭되는 link 자체가 근처에 없음). 나머지
세그먼트도 speed 값을 갖게 하기 위해, 그 세그먼트 자신의 geometry를
link_points로, LION의 POSTED_SPEED(제한속도, 없으면 뉴욕시 기본값
25mph)에 무작위 변동을 준 값을 speed로 삼아 실제 API 응답과 동일한
스키마의 row를 만든다. 제한속도라도 실제로는 정체로 더 느릴 수도,
자유흐름으로 더 빠를 수도 있어서 그대로 쓰지 않고 변동을 준다.
"""

from __future__ import annotations

import random

import geopandas as gpd
import pandas as pd
from shapely import wkt as shapely_wkt

from src.common.config import GOLD2_DIR, LION_CRS
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_synthetic")

DEFAULT_SPEED_MPH = 25.0  # NYC 기본 제한속도(Vision Zero 정책 기준값)
_SPEED_VARIATION_MIN = 0.3
_SPEED_VARIATION_MAX = 1.3
_MIN_SPEED_MPH = 1.0

_LATLON_CRS = "EPSG:4326"

_BOROUGH_NAMES = {
    "1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island",
}

# src/speed/bronze.py가 실제 API에서 받는 컬럼과 동일한 순서/이름.
SPEED_COLUMNS = [
    "id", "speed", "travel_time", "status", "data_as_of", "link_id",
    "link_points", "encoded_poly_line", "encoded_poly_line_lvls", "owner",
    "transcom_id", "borough", "link_name",
]

# 참고표(reference table) 스키마 - routable 세그먼트마다 무거운 계산
# (geometry->link_points 변환, POSTED_SPEED 조회)을 미리 끝내둔 결과.
REFERENCE_TABLE_COLUMNS = [
    "segment_id", "link_points", "base_speed", "street_name", "borough", "length_ft",
]

# LION은 분기(1월/7월)에 한 번만 갱신되는데 collect_speed_data()는 30분마다
# 도니, 매번 gdb 원본(로드만 9초+)을 다시 읽지 않도록 이 경로에 캐싱한다.
# dim_segment.parquet(Gold2)과 같은 디렉터리에 둔다 - 같은 LION 소스에서
# 파생된 산출물이라서다.
REFERENCE_TABLE_PATH = GOLD2_DIR / "speed_synthetic_reference.parquet"


def _clean_posted_speed(raw_df: pd.DataFrame) -> pd.Series:
    """LION 원본의 SegmentID/POSTED_SPEED 컬럼을 segment_id -> mph 시리즈로
    정리한다. POSTED_SPEED 결측은 NaN이 아니라 공백 문자열("  ")로 들어있어서
    (routable 세그먼트의 18.8%가 이 상태) 여기서 명시적으로 NaN 처리한다."""

    speed = pd.to_numeric(raw_df["POSTED_SPEED"].str.strip(), errors="coerce")
    return pd.Series(speed.values, index=raw_df["SegmentID"].values)


def load_posted_speed(lion_gdb_path) -> pd.Series:
    """LION 원본 gdb에서 SegmentID -> POSTED_SPEED(mph)를 읽는다.

    dim_segment(Silver1/Gold2 산출물)엔 이 컬럼이 없어서 원본에서 직접 읽는다.
    """
    gdf = gpd.read_file(str(lion_gdb_path), layer="lion", columns=["SegmentID", "POSTED_SPEED"])
    gdf = gdf.drop_duplicates(subset="SegmentID", keep="first")
    return _clean_posted_speed(gdf)


def segment_geometry_to_link_points(geometry_wkt: str) -> str:
    """LION segment geometry(WKT, EPSG:2263 feet)를 speed API의 link_points
    포맷("위도,경도 위도,경도 ...", EPSG:4326)으로 변환한다.

    MultiLineString이면 구성 LineString들의 좌표를 순서대로 이어붙인다.
    """
    geometry = shapely_wkt.loads(geometry_wkt)
    reprojected = gpd.GeoSeries([geometry], crs=LION_CRS).to_crs(_LATLON_CRS).iloc[0]

    lines = list(reprojected.geoms) if reprojected.geom_type == "MultiLineString" else [reprojected]
    coords = [pt for line in lines for pt in line.coords]

    return " ".join(f"{lat:.7f},{lon:.7f}" for lon, lat in coords)


def _random_speed(base_speed: float, rng: random.Random) -> float:
    multiplier = rng.uniform(_SPEED_VARIATION_MIN, _SPEED_VARIATION_MAX)
    return max(_MIN_SPEED_MPH, base_speed * multiplier)


def build_reference_table(dim_segment_df: pd.DataFrame, posted_speed: pd.Series) -> pd.DataFrame:
    """routable 세그먼트 전체에 대해 geometry->link_points 변환과
    POSTED_SPEED(없으면 기본값) 조회를 미리 끝내둔 참고표를 만든다.

    이게 이 모듈에서 제일 무거운 계산이라(세그먼트당 shapely/geopandas
    호출) load_or_build_reference_table()이 결과를 캐싱해서 재사용한다."""

    rows = []
    for _, seg in dim_segment_df.iterrows():
        segment_id = seg["segment_id"]
        base_speed = posted_speed.get(segment_id)
        if base_speed is None or pd.isna(base_speed) or base_speed <= 0:
            base_speed = DEFAULT_SPEED_MPH

        rows.append({
            "segment_id": segment_id,
            "link_points": segment_geometry_to_link_points(seg["geometry"]),
            "base_speed": base_speed,
            "street_name": seg.get("street_name", ""),
            "borough": _BOROUGH_NAMES.get(str(seg.get("borough_code", "")), ""),
            "length_ft": seg.get("length_ft") or 0.0,
        })

    logger.info(f"[speed_synthetic] 참고표 {len(rows)}행 생성")

    return pd.DataFrame(rows, columns=REFERENCE_TABLE_COLUMNS)


def load_or_build_reference_table(
    reference_path=REFERENCE_TABLE_PATH,
    *,
    dim_segment_loader,
    posted_speed_loader,
) -> pd.DataFrame:
    """참고표 캐시가 있으면 그대로 읽고(gdb 재로드 없음), 없으면 한 번
    만들어서 저장한다. dim_segment_loader/posted_speed_loader는 캐시가
    없을 때만 호출된다(무거운 gdb 읽기를 캐시 hit 시 완전히 건너뛰기 위함)."""

    if reference_path.exists():
        return pd.read_parquet(str(reference_path))

    dim_segment = dim_segment_loader()
    routable = dim_segment[dim_segment["is_routable"]] if "is_routable" in dim_segment else dim_segment
    posted_speed = posted_speed_loader()

    table = build_reference_table(routable, posted_speed)

    reference_path.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(str(reference_path), index=False)
    logger.info(f"[speed_synthetic] 참고표 캐시 저장 -> {reference_path}")

    return table


def build_synthetic_rows(
    reference_df: pd.DataFrame,
    uncovered_segment_ids,
    data_as_of: str,
    rng: random.Random | None = None,
) -> pd.DataFrame:
    """참고표(build_reference_table 산출물)를 바탕으로, 커버 안 된
    세그먼트들에 대해 실제 속도 API와 동일한 스키마의 synthetic row를
    만든다. geometry 변환 등 무거운 계산은 참고표에 이미 끝나있어서
    여기서는 매번 달라지는 speed 변동만 계산한다."""

    rng = rng or random.Random()
    uncovered_set = set(uncovered_segment_ids)
    target = reference_df[reference_df["segment_id"].isin(uncovered_set)]

    rows = []
    for _, ref in target.iterrows():
        segment_id = ref["segment_id"]
        speed = _random_speed(ref["base_speed"], rng)
        length_ft = ref["length_ft"] or 0.0
        travel_time = (length_ft / (speed * 5280 / 3600)) if speed > 0 else 0.0

        rows.append({
            # 실제 API는 Socrata 특성상 모든 필드가 문자열로 온다
            # (예: "speed":"29.82") - 합쳤을 때 dtype이 섞이지 않도록
            # 숫자 필드도 문자열로 맞춘다.
            "id": str(segment_id),
            "speed": f"{speed:.2f}",
            "travel_time": f"{travel_time:.0f}",
            "status": "0",
            "data_as_of": data_as_of,
            "link_id": segment_id,
            "link_points": ref["link_points"],
            "encoded_poly_line": "",
            "encoded_poly_line_lvls": "",
            "owner": "NYC-DOT",
            "transcom_id": str(segment_id),
            "borough": ref["borough"],
            "link_name": ref["street_name"],
        })

    logger.info(f"[speed_synthetic] synthetic row {len(rows)}개 생성")

    return pd.DataFrame(rows, columns=SPEED_COLUMNS)
