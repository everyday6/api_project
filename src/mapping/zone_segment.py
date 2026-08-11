"""
Silver 매핑: dim_segment(LION) x TLC Taxi Zone -> map_zone_segment

도로 구간(LineString)과 zone(Polygon)을 매핑하는 방법을 정했다 (여러 안 중 택함):
- 세그먼트 중점(midpoint)이 어느 zone 폴리곤 안에 있는지로 1:1 매칭한다.
  실제 도로는 zone 경계를 가로지르는 경우가 흔해서 "교차(intersects)" 기준으로
  하면 세그먼트당 여러 zone이 나와 PK(segment_id)가 유일하지 않게 된다 — 이후
  Traffic Score 계산에서 "이 구간은 이 zone 소속"이 명확해야 하므로 1:1을 택함.
- non_routable(전체의 약 28%, 보행로/페리/경계선 등)은 애초에 Traffic Score
  계산 대상이 아니라서 매핑하지 않는다. dim_segment에서 is_routable=True만 사용.
- 지오메트리 라이브러리는 shapely만 쓴다. geopandas는 컨테이너에 없어서 새로
  설치해야 하는데(shapely는 이미 있음), 이 정도 규모(15만 행, 263개 zone)에는
  WKT 직접 파싱 + STRtree 공간인덱스로 충분하다.

좌표계는 dim_segment(LION)와 TLC Taxi Zone shapefile이 둘 다 EPSG:2263(NAD83 /
New York Long Island, US feet)라서 재투영이 필요 없다(직접 ogrinfo로 확인함).

미매칭(약 0.6%, 989/157153 — 직접 확인): zone 폴리곤 사이 틈(다리 구간 등)에
중점이 떨어지는 경우로 추정된다. 이 버전에서는 결과 테이블에서 제외하고 로그만
남긴다 — nearest-zone fallback 같은 보정은 필요해지면 추가한다.
"""

from __future__ import annotations

import io
import subprocess
from pathlib import Path

import pandas as pd
from shapely import wkt
from shapely.strtree import STRtree

from src.common.config import BRONZE_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_zone_segment")

TAXI_ZONE_SHAPEFILE = BRONZE_DIR / "taxi_zone" / "shapefile" / "taxi_zones" / "taxi_zones.shp"
MAP_ZONE_SEGMENT_PATH = SILVER_DIR / "map_zone_segment.parquet"

ZONE_COLUMNS = ["LocationID", "borough"]


def _load_zones(shapefile_path: Path) -> pd.DataFrame:
    """
    TLC Taxi Zone shapefile을 읽어 (zone_id, borough, geom) 테이블로 만든다.
    LION과 마찬가지로 CONVERT_TO_LINEAR를 걸어 곡선 geometry가 섞여도 shapely가
    읽을 수 있게 한다(폴리곤에서는 흔치 않지만 방어적으로 동일하게 처리).
    """
    cmd = [
        "ogr2ogr",
        "-f", "CSV",
        "/vsistdout/",
        str(shapefile_path),
        "-select", ",".join(ZONE_COLUMNS),
        "-lco", "GEOMETRY=AS_WKT",
        "-nlt", "CONVERT_TO_LINEAR",
    ]
    logger.info(f"[map_zone_segment] zone shapefile 읽기: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"[map_zone_segment] zone shapefile 읽기 실패: {result.stderr}")
        raise RuntimeError(f"zone shapefile 변환 실패: {result.stderr}")

    zones = pd.read_csv(io.StringIO(result.stdout))
    zones["geom"] = zones["WKT"].apply(wkt.loads)
    return zones[["LocationID", "borough", "geom"]]


def build_map_zone_segment(
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
    silver_root: Path = SILVER_DIR,
) -> str:
    """dim_segment(routable만)와 Taxi Zone을 중점 기준 1:1로 매핑한다."""

    segments = pd.read_parquet(dim_segment_path)
    segments = segments.loc[segments["is_routable"], ["segment_id", "geometry"]].copy()
    logger.info(f"[map_zone_segment] 매핑 대상(is_routable=True): {len(segments)}행")

    zones = _load_zones(zone_shapefile_path)
    tree = STRtree(zones["geom"].tolist())

    midpoints = segments["geometry"].apply(lambda g: wkt.loads(g).interpolate(0.5, normalized=True))

    zone_ids = []
    boroughs = []
    unmatched = 0
    multi_match = 0

    for mid in midpoints:
        idxs = tree.query(mid, predicate="intersects")
        if len(idxs) == 0:
            unmatched += 1
            zone_ids.append(None)
            boroughs.append(None)
            continue
        if len(idxs) > 1:
            multi_match += 1
        row = zones.iloc[idxs[0]]
        zone_ids.append(row["LocationID"])
        boroughs.append(row["borough"])

    segments["zone_id"] = zone_ids
    segments["borough"] = boroughs

    if multi_match:
        logger.warning(f"[map_zone_segment] 중점이 zone 경계에 걸쳐 2개 이상 매칭된 행 {multi_match}개 (첫 번째로 결정)")
    if unmatched:
        logger.warning(f"[map_zone_segment] zone을 못 찾은 세그먼트 {unmatched}개 (결과에서 제외)")

    map_zone_segment = (
        segments.dropna(subset=["zone_id"])
        .assign(zone_id=lambda d: d["zone_id"].astype(int))
        [["segment_id", "zone_id", "borough"]]
    )

    silver_root.mkdir(parents=True, exist_ok=True)
    map_zone_segment_path = silver_root / "map_zone_segment.parquet"
    map_zone_segment.to_parquet(map_zone_segment_path, index=False)

    logger.info(f"[map_zone_segment] {len(map_zone_segment)}행 저장 -> {map_zone_segment_path}")
    return str(map_zone_segment_path)


def validate_map_zone_segment(path: str, dim_segment_path: Path = DIM_SEGMENT_PATH) -> str:
    """map_zone_segment.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (1:1 매핑 깨짐)"
    assert df["zone_id"].between(1, 263).all(), "zone_id가 TLC 공식 범위(1~263) 밖입니다"
    assert df["borough"].notna().all(), "borough가 비어있는 행 발견"

    # 매칭률은 "지금 이 실행"의 routable 세그먼트 수 대비로 계산한다 — 분기마다
    # LION 행 수 자체가 바뀌므로 예전 실행의 숫자를 하드코딩하면 의미가 없어진다.
    routable_total = int(pd.read_parquet(dim_segment_path, columns=["is_routable"])["is_routable"].sum())
    match_rate = len(df) / routable_total
    assert match_rate >= 0.95, (
        f"매칭률이 비정상적으로 낮습니다: {match_rate:.1%} ({len(df)}/{routable_total}, 직접 확인한 기준 약 99.4%)"
    )

    logger.info(f"[map_zone_segment] 검증 통과 ({len(df)}행, 매칭률 {match_rate:.1%})")
    return path


if __name__ == "__main__":
    out = build_map_zone_segment()
    validate_map_zone_segment(out)
