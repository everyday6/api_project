"""LION segment와 TLC Taxi Zone을 1:1로 연결하는 Silver2 매핑."""

from __future__ import annotations

import io
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import pandas as pd
from shapely import wkt
from shapely.strtree import STRtree

from src.common.config import SILVER1_DIR, SILVER2_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_zone_segment")

LION_SEGMENT_PATH = SILVER1_DIR / "dim_segment.parquet"
TAXI_ZONE_SHAPEFILE = (
    SILVER1_DIR / "taxi_zone" / "shapefile" / "taxi_zones" / "taxi_zones.shp"
)
MAP_ZONE_SEGMENT_PATH = SILVER2_DIR / "map_zone_segment.parquet"
MAP_ZONE_SEGMENT_STAGING_ROOT = SILVER2_DIR / "_staging" / "map_zone_segment"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def _staging_run_path(run_id: str, staging_root=MAP_ZONE_SEGMENT_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 zone-segment staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


def validate_reference_inputs(
    lion_segment_path=LION_SEGMENT_PATH,
    zone_shapefile_path=TAXI_ZONE_SHAPEFILE,
) -> dict:
    """운영 매핑에 필요한 두 Silver1 입력의 존재를 확인한다."""

    missing = []
    if not lion_segment_path.exists():
        missing.append(f"LION Silver1={lion_segment_path}")
    if not zone_shapefile_path.exists():
        missing.append(f"Taxi Zone Silver1={zone_shapefile_path}")
    if missing:
        raise FileNotFoundError(
            "Zone-Segment 필수 입력이 없습니다: " + ", ".join(missing)
        )

    logger.info(
        "Zone-Segment 입력 확인 완료: lion=%s taxi_zone=%s",
        lion_segment_path,
        zone_shapefile_path,
    )
    return {
        "lion_segment_path": str(lion_segment_path),
        "zone_shapefile_path": str(zone_shapefile_path),
    }


def _stage_shapefile_locally(shapefile_path, work_dir: Path) -> Path:
    if isinstance(shapefile_path, Path):
        return shapefile_path

    local_dir = work_dir / shapefile_path.parent.name
    downloaded_dir = Path(shapefile_path.parent.download_to(local_dir))
    local_shapefile = downloaded_dir / shapefile_path.name
    required = [
        local_shapefile,
        local_shapefile.with_suffix(".dbf"),
        local_shapefile.with_suffix(".shx"),
    ]
    missing = [path.name for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Taxi Zone Shapefile 로컬 다운로드 누락: {missing}")
    return local_shapefile


def _load_zones_from_local(shapefile_path: Path) -> pd.DataFrame:
    command = [
        "ogr2ogr",
        "-f", "CSV",
        "/vsistdout/",
        str(shapefile_path),
        "-select", "LocationID,borough",
        "-lco", "GEOMETRY=AS_WKT",
        "-nlt", "CONVERT_TO_LINEAR",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Taxi Zone Shapefile 변환 실패: {result.stderr}")

    zones = pd.read_csv(io.StringIO(result.stdout))
    zones["geometry"] = zones["WKT"].map(wkt.loads)
    return zones.rename(columns={"LocationID": "zone_id"})[
        ["zone_id", "borough", "geometry"]
    ]


def _load_zones(shapefile_path) -> pd.DataFrame:
    if isinstance(shapefile_path, Path):
        return _load_zones_from_local(shapefile_path)

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="taxi_zone_mapping_", dir=TMP_DIR) as tmp:
        local_path = _stage_shapefile_locally(shapefile_path, Path(tmp))
        return _load_zones_from_local(local_path)


def _map_segments_to_zones(
    segments: pd.DataFrame,
    zones: pd.DataFrame,
) -> pd.DataFrame:
    """segment 중점을 포함하는 zone에 매핑하고, 경계 틈은 최근접 zone으로 보완한다."""

    required_segments = {"segment_id", "geometry"}
    required_zones = {"zone_id", "borough", "geometry"}
    if missing := required_segments - set(segments.columns):
        raise ValueError(f"LION segment 필수 컬럼 없음: {missing}")
    if missing := required_zones - set(zones.columns):
        raise ValueError(f"Taxi Zone 필수 컬럼 없음: {missing}")
    if zones.empty:
        raise ValueError("Taxi Zone 데이터가 비어 있습니다")
    if segments["segment_id"].duplicated().any():
        raise ValueError("LION segment_id 중복 발견")
    if segments["geometry"].isna().any():
        raise ValueError("geometry가 없는 LION segment가 있습니다")

    zone_frame = zones.reset_index(drop=True).copy()
    zone_geometries = zone_frame["geometry"].tolist()
    tree = STRtree(zone_geometries)
    records = []

    for row in segments[["segment_id", "geometry"]].itertuples(index=False):
        line = wkt.loads(row.geometry) if isinstance(row.geometry, str) else row.geometry
        midpoint = line.interpolate(0.5, normalized=True)
        candidates = list(tree.query(midpoint, predicate="intersects"))

        if candidates:
            # zone 경계 위에 정확히 놓인 경우에도 실행마다 같은 zone을 고른다.
            zone_index = min(candidates, key=lambda index: int(zone_frame.iloc[index]["zone_id"]))
            method = "contains"
            distance_ft = 0.0
        else:
            zone_index = int(tree.nearest(midpoint))
            method = "nearest"
            distance_ft = float(midpoint.distance(zone_geometries[zone_index]))

        zone = zone_frame.iloc[zone_index]
        records.append({
            "segment_id": str(row.segment_id),
            "zone_id": int(zone["zone_id"]),
            "borough": zone["borough"],
            "mapping_method": method,
            "distance_ft": distance_ft,
        })

    return pd.DataFrame.from_records(records, columns=[
        "segment_id", "zone_id", "borough", "mapping_method", "distance_ft",
    ])


def build_map_zone_segment_staged(
    lion_segment_path=LION_SEGMENT_PATH,
    zone_shapefile_path=TAXI_ZONE_SHAPEFILE,
    staging_root=MAP_ZONE_SEGMENT_STAGING_ROOT,
) -> dict:
    """LION과 Taxi Zone을 연결해 실행별 임시 경로에 저장한다."""

    segments = pd.read_parquet(str(lion_segment_path), columns=["segment_id", "geometry"])
    zones = _load_zones(zone_shapefile_path)
    mapping = _map_segments_to_zones(segments, zones)

    run_id = uuid4().hex
    run_path = _staging_run_path(run_id, staging_root)
    stage_path = run_path / "map_zone_segment.parquet"
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    mapping.to_parquet(str(stage_path), index=False)
    logger.info(
        "segment-zone staging 저장 완료: %s행(nearest=%s행) -> %s",
        len(mapping),
        int((mapping["mapping_method"] == "nearest").sum()),
        stage_path,
    )
    return {"run_id": run_id, "stage_path": str(stage_path)}


def validate_map_zone_segment(path, lion_segment_path=LION_SEGMENT_PATH) -> str:
    """모든 입력 segment가 정확히 하나의 유효한 zone을 갖는지 검증한다."""

    mapping = pd.read_parquet(str(path))
    segment_count = len(pd.read_parquet(str(lion_segment_path), columns=["segment_id"]))

    if len(mapping) != segment_count:
        raise ValueError(f"segment coverage 불일치: {len(mapping)}/{segment_count}")
    if not mapping["segment_id"].is_unique:
        raise ValueError("Silver2 segment_id 중복 발견")
    if mapping[["segment_id", "zone_id", "borough"]].isna().any().any():
        raise ValueError("Silver2 필수값 NULL 발견")
    if not mapping["zone_id"].between(1, 263).all():
        raise ValueError("zone_id가 공식 범위(1~263) 밖입니다")
    if not mapping["mapping_method"].isin(["contains", "nearest"]).all():
        raise ValueError("알 수 없는 mapping_method 발견")

    logger.info("segment-zone Silver2 검증 통과: %s행", len(mapping))
    return str(path)


def validate_staged_map_zone_segment(
    stage_result: dict,
    lion_segment_path=LION_SEGMENT_PATH,
    staging_root=MAP_ZONE_SEGMENT_STAGING_ROOT,
) -> dict:
    """임시 매핑 경로와 데이터 품질을 검증한다."""

    expected_path = (
        _staging_run_path(stage_result["run_id"], staging_root)
        / "map_zone_segment.parquet"
    )
    if stage_result.get("stage_path") != str(expected_path):
        raise ValueError("예상하지 못한 zone-segment staging 경로입니다")
    validate_map_zone_segment(expected_path, lion_segment_path)
    return stage_result


def publish_map_zone_segment(
    validated_stage: dict,
    output_path=MAP_ZONE_SEGMENT_PATH,
    staging_root=MAP_ZONE_SEGMENT_STAGING_ROOT,
) -> str:
    """검증된 임시 매핑만 운영 Silver2 경로로 승격하고 임시본을 지운다."""

    run_path = _staging_run_path(validated_stage["run_id"], staging_root)
    stage_path = run_path / "map_zone_segment.parquet"
    if validated_stage.get("stage_path") != str(stage_path):
        raise ValueError("예상하지 못한 zone-segment staging 경로입니다")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(stage_path, Path):
        shutil.copy2(stage_path, output_path)
    else:
        stage_path.copy(output_path)
    if not output_path.exists():
        raise RuntimeError(f"zone-segment 운영 경로 승격 실패: {output_path}")

    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("segment-zone Silver2 승격 완료: %s", output_path)
    return str(output_path)
