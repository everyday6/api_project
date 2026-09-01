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
from src.common.utils import save_parquet
from src.toll.bronze import BRONZE_ROOT

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_silver2")

MAP_LION_FACILITY_PATH = SILVER2_DIR / "map_lion_facility.parquet"


def load_lion_segments(gdb_path: Path) -> gpd.GeoDataFrame:
    """LION Bronze GDB에서 segment_id/street/geometry만 뽑는다.

    LION 원본은 같은 segment_id가 여러 행으로 중복돼 있다(실측:
    243,237행 중 고유 segment_id는 218,373개 — 약 2.5만 건 중복). 원래
    lion/silver1.py가 이 dedup을 해줬는데 지금은 lion 도메인이 Bronze만
    있어서 이 파일에서 직접 처리한다(조용히 첫 번째 행만 남김, 기존
    lion/silver1.py와 동일한 정책).

    GDB 전체(24만 행 이상)를 geopandas로 읽는 게 이 파이프라인에서 제일
    오래 걸리는 부분이라(컨테이너 안 볼륨 마운트로 읽으면 더 오래 걸림 —
    실제로 겪음), 시작/완료를 각각 로그로 남긴다."""

    logger.info(f"[toll_silver2] LION GDB 읽기 시작: {gdb_path}")
    gdf = gpd.read_file(gdb_path, layer="lion")
    logger.info(f"[toll_silver2] LION GDB 읽기 완료: {len(gdf)}행")

    gdf = gdf.rename(columns={"SegmentID": "segment_id", "Street": "street"})
    gdf = gdf.drop_duplicates(subset="segment_id", keep="first")
    logger.info(f"[toll_silver2] segment_id 중복 제거 완료: {len(gdf)}행")

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


def validate_lion_facility_mapping(mapping: pd.DataFrame, facility_keys: set[str]) -> None:
    """lion_facility 매핑을 저장하기 전 크리티컬 검증. 실패하면 ValueError를
    던져 Airflow task를 실패시킨다(저장 안 함 -> 기존 parquet 유지, 정합성
    우선 - RELIABILITY_PRINCIPLES.md Tier 0-A).

    toll 매핑은 (segment_id, facility_key)뿐이라 행 단위 "이상치" 신호가
    약하다(is_suspect 대상 아님) - 대신 아래 4가지가 깨지면 Gold 요금 계산이
    조용히 틀리므로 여기서 막는다.
    """
    if mapping.empty:
        raise ValueError(
            "lion_facility 매핑 결과가 비어 있습니다 - toll_facilities.yaml 패턴이나 "
            "LION GDB street 컬럼을 확인하세요"
        )
    if mapping["segment_id"].isna().any() or (mapping["segment_id"].astype(str).str.strip() == "").any():
        raise ValueError("lion_facility 매핑에 segment_id가 비어 있는 행이 있습니다")
    if mapping.duplicated(subset=["segment_id", "facility_key"]).any():
        raise ValueError("lion_facility 매핑에 (segment_id, facility_key) 중복이 있습니다")
    unknown = set(mapping["facility_key"]) - facility_keys
    if unknown:
        raise ValueError(
            f"lion_facility 매핑에 toll_facilities.yaml에 없는 facility_key가 있습니다: {sorted(unknown)}"
        )


def build_lion_facility_mapping(
    gdb_path: Path,
    facilities_path: Path = BRONZE_ROOT / "toll_facilities.yaml",
    out_path: Path = MAP_LION_FACILITY_PATH,
) -> str:
    logger.info(f"[toll_silver2] lion_facility 매핑 시작 (facilities={facilities_path})")

    segments = load_lion_segments(gdb_path)

    logger.info(f"[toll_silver2] {len(segments)}개 segment 대상 시설명 매칭 시작")
    result = match_lion_facilities(segments, facilities_path)

    facility_keys = set(yaml.safe_load(Path(facilities_path).read_text()))
    validate_lion_facility_mapping(result, facility_keys)

    save_parquet(result, out_path.parent, out_path.name)

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


def validate_lion_cbd_mapping(mapping: pd.DataFrame) -> None:
    """lion_cbd 매핑을 저장하기 전 크리티컬 검증. 실패하면 ValueError를 던진다
    (저장 안 함, 정합성 우선 - RELIABILITY_PRINCIPLES.md Tier 0-A).

    빈 결과 검사가 핵심이다 - CBD zone(맨해튼 60번가 이남)에는 수백 개
    segment가 들어가므로 0건은 정상일 수 없고, match_lion_cbd 주석이
    경고하듯 좌표계 불일치 시 gpd.sjoin이 경고만 내고 조용히 0건을 반환하는
    함정을 여기서 막는다.
    """
    if mapping.empty:
        raise ValueError(
            "lion_cbd 매핑 결과가 비어 있습니다 - CBD Geofence와 LION segment의 "
            "좌표계(CRS) 불일치일 가능성이 높습니다(match_lion_cbd 주석 참고)"
        )
    if mapping["segment_id"].isna().any() or (mapping["segment_id"].astype(str).str.strip() == "").any():
        raise ValueError("lion_cbd 매핑에 segment_id가 비어 있는 행이 있습니다")
    if mapping["segment_id"].duplicated().any():
        raise ValueError("lion_cbd 매핑에 segment_id 중복이 있습니다")


def build_lion_cbd_mapping(
    gdb_path: Path,
    cbd_geofence_path: Path = BRONZE_ROOT / "cbd_geofence.geojson",
    out_path: Path = MAP_LION_CBD_PATH,
) -> str:
    logger.info(f"[toll_silver2] lion_cbd 매핑 시작 (cbd_geofence={cbd_geofence_path})")

    segments = load_lion_segments(gdb_path)
    zone_polygon = gpd.read_file(cbd_geofence_path)

    logger.info(f"[toll_silver2] {len(segments)}개 segment 대상 CBD 공간조인 시작")
    result = match_lion_cbd(segments, zone_polygon)

    validate_lion_cbd_mapping(result)

    save_parquet(result, out_path.parent, out_path.name)

    logger.info(f"[toll_silver2] lion_cbd 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
