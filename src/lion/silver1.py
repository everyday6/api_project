"""LION Bronze 스냅샷을 정제해 Silver1 segment 기준정보로 만든다."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4

import pandas as pd
from cloudpathlib import S3Path

from src.common.config import BRONZE_DIR, SILVER1_DIR, TMP_DIR
from src.common.logger import get_logger
from src.common.utils import clean_street


logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_silver1")

LION_BRONZE_ROOT = BRONZE_DIR / "lion"
LION_SILVER1_ROOT = SILVER1_DIR / "lion"
DIM_SEGMENT_PATH = LION_SILVER1_ROOT / "dim_segment.parquet"
DIM_SEGMENT_STAGING_ROOT = LION_SILVER1_ROOT / "_staging"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")

LION_COLUMNS = [
    "SegmentID",
    "Street",
    "RW_TYPE",
    "TRUCK_ROUTE_TYPE",
    "TrafDir",
    "FeatureTyp",
    "Number_Travel_Lanes",
    "Number_Total_Lanes",
    "StreetWidth_Min",
    "StreetWidth_Max",
    "SHAPE_Length",
    "LBoro",
    "NodeIDFrom",
    "NodeIDTo",
]
SILVER_COLUMNS = [
    "segment_id",
    "street_name",
    "borough_code",
    "geometry",
    "length_ft",
    "lanes_total",
    "node_from",
    "node_to",
    "RW_TYPE",
    "TRUCK_ROUTE_TYPE",
    "TrafDir",
    "FeatureTyp",
]
VALID_BOROUGH_CODES = {"", "1", "2", "3", "4", "5"}
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


def _as_path(value):
    if isinstance(value, (Path, S3Path)):
        return value
    text = str(value)
    return S3Path(text) if text.startswith("s3://") else Path(text)


def _staging_run_path(run_id: str, staging_root=DIM_SEGMENT_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 LION Silver1 staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


def _find_gdb(version_dir: Path) -> Path:
    gdbs = sorted(version_dir.rglob("*.gdb"))
    if not gdbs:
        raise FileNotFoundError(f"{version_dir} 안에 .gdb가 없습니다")
    return gdbs[0]


def _stage_bronze_locally(version_dir, work_dir: Path) -> Path:
    """GDAL이 읽을 수 있도록 S3 Bronze 스냅샷을 로컬에 준비한다."""

    if isinstance(version_dir, Path):
        return version_dir
    return Path(version_dir.download_to(work_dir / "bronze_version"))


def _gdb_to_flat_csv(gdb_path: Path, output_path: Path) -> Path:
    command = [
        "ogr2ogr",
        "-f",
        "CSV",
        str(output_path),
        str(gdb_path),
        "lion",
        "-select",
        ",".join(LION_COLUMNS),
        "-lco",
        "GEOMETRY=AS_WKT",
        "-nlt",
        "CONVERT_TO_LINEAR",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"LION ogr2ogr 변환 실패: {result.stderr}")
    if not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError("LION ogr2ogr 결과가 비어 있습니다")
    return output_path


def _transform_lion_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """원본 컬럼을 Silver1 공통 segment 스키마로 정제한다."""

    if "SHAPE" not in frame.columns:
        if "WKT" not in frame.columns:
            raise ValueError("LION geometry 컬럼(SHAPE/WKT)이 없습니다")
        frame = frame.rename(columns={"WKT": "SHAPE"})

    missing = set(LION_COLUMNS) - {"SHAPE"} - set(frame.columns)
    if missing:
        raise ValueError(f"LION 필수 컬럼이 없습니다: {sorted(missing)}")

    cleaned = frame.copy()
    for column in ["SegmentID", "LBoro", "RW_TYPE", "TRUCK_ROUTE_TYPE"]:
        cleaned[column] = cleaned[column].astype(str).str.strip()
    cleaned["Number_Travel_Lanes"] = pd.to_numeric(
        cleaned["Number_Travel_Lanes"].str.strip(),
        errors="coerce",
    )
    cleaned["SHAPE_Length"] = pd.to_numeric(
        cleaned["SHAPE_Length"].str.strip(),
        errors="coerce",
    )
    cleaned["street_name"] = cleaned["Street"].map(clean_street)
    cleaned = cleaned.drop_duplicates(subset="SegmentID", keep="first")

    return cleaned.rename(columns={
        "SegmentID": "segment_id",
        "LBoro": "borough_code",
        "SHAPE": "geometry",
        "SHAPE_Length": "length_ft",
        "Number_Travel_Lanes": "lanes_total",
        "NodeIDFrom": "node_from",
        "NodeIDTo": "node_to",
    })[SILVER_COLUMNS]


def build_dim_segment_staged(
    bronze_version_path: str,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """이번 LION Bronze 스냅샷을 실행별 Silver1 임시 경로에 저장한다."""

    version_dir = _as_path(bronze_version_path)
    marker = version_dir / "_metadata.txt"
    if not marker.exists():
        raise FileNotFoundError(f"완료되지 않은 LION Bronze 스냅샷입니다: {version_dir}")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_silver1_", dir=TMP_DIR) as tmp:
        work_dir = Path(tmp)
        local_version = _stage_bronze_locally(version_dir, work_dir)
        csv_path = _gdb_to_flat_csv(_find_gdb(local_version), work_dir / "lion.csv")
        source = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        dim_segment = _transform_lion_frame(source)

    run_id = uuid4().hex
    run_path = _staging_run_path(run_id, staging_root)
    stage_path = run_path / "dim_segment.parquet"
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(stage_path), index=False)
    logger.info(
        "LION Silver1 staging 저장 완료: rows=%s source=%s path=%s",
        len(dim_segment),
        version_dir,
        stage_path,
    )
    return {
        "run_id": run_id,
        "stage_path": str(stage_path),
        "source_version": str(version_dir),
    }


def validate_dim_segment(path, min_rows=MIN_EXPECTED_ROWS, max_rows=MAX_EXPECTED_ROWS) -> str:
    """Silver1 segment 기준정보의 운영 불변식을 확인한다."""

    frame = pd.read_parquet(str(path))
    missing = set(SILVER_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"LION Silver1 필수 컬럼이 없습니다: {sorted(missing)}")
    if not min_rows <= len(frame) <= max_rows:
        raise ValueError(f"LION Silver1 행 수가 예상 범위 밖입니다: {len(frame)}")
    if frame["segment_id"].isna().any() or (frame["segment_id"] == "").any():
        raise ValueError("LION Silver1 segment_id NULL/빈 값 발견")
    if not frame["segment_id"].is_unique:
        raise ValueError("LION Silver1 segment_id 중복 발견")
    if frame["geometry"].isna().any() or (frame["geometry"] == "").any():
        raise ValueError("LION Silver1 geometry NULL/빈 값 발견")
    invalid_boroughs = set(frame["borough_code"].astype(str)) - VALID_BOROUGH_CODES
    if invalid_boroughs:
        raise ValueError(f"LION Silver1 알 수 없는 borough_code: {sorted(invalid_boroughs)}")
    logger.info("LION Silver1 검증 통과: rows=%s path=%s", len(frame), path)
    return str(path)


def validate_staged_dim_segment(
    stage_result: dict,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
    min_rows=MIN_EXPECTED_ROWS,
    max_rows=MAX_EXPECTED_ROWS,
) -> dict:
    expected_path = (
        _staging_run_path(stage_result["run_id"], staging_root)
        / "dim_segment.parquet"
    )
    if stage_result.get("stage_path") != str(expected_path):
        raise ValueError("예상하지 못한 LION Silver1 staging 경로입니다")
    validate_dim_segment(expected_path, min_rows=min_rows, max_rows=max_rows)
    return stage_result


def publish_dim_segment(
    validated_stage: dict,
    output_path=DIM_SEGMENT_PATH,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """검증된 LION Silver1 단일 객체를 운영 경로에 반영한다."""

    run_id = validated_stage["run_id"]
    stage_path = _staging_run_path(run_id, staging_root) / "dim_segment.parquet"
    if validated_stage.get("stage_path") != str(stage_path):
        raise ValueError("예상하지 못한 LION Silver1 staging 경로입니다")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(stage_path, Path):
        shutil.copy2(stage_path, output_path)
    else:
        stage_path.copy(output_path)
    if not output_path.exists():
        raise RuntimeError(f"LION Silver1 운영 경로 반영 실패: {output_path}")

    logger.info("LION Silver1 운영 반영 완료: %s", output_path)
    return {**validated_stage, "output_path": str(output_path)}


def cleanup_dim_segment_staging(
    published_result: dict,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> None:
    run_path = _staging_run_path(published_result["run_id"], staging_root)
    if not run_path.exists():
        return
    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("LION Silver1 staging 정리 완료: %s", run_path)
