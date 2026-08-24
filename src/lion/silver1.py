"""
Silver1 변환: LION bronze -> dim_segment(기본 컬럼)

구조적 정제(컬럼명 통일, 타입 캐스팅, 도로명 정규화, SegmentID dedupe)만
한다. is_routable 계산은 src/lion/gold2.py가 이 산출물을 읽어서 한다 —
그 계산에 필요한 원본 코드 컬럼(RW_TYPE, FeatureTyp)은 이름 그대로
통과시켜 둔다. POSTED_SPEED(제한속도)도 같은 이유로 통과시키되, type1
SPEC Estimate 폴백(src/nav_time/gold2.py)이 바로 쓸 수 있게 speed_limit_mph로
이름만 바꾼다.

pandas를 쓰는 이유: LION은 분기 1회 갱신되는 24만 행짜리 참조 테이블이라
이 컴퓨터 한 대의 메모리로 몇 초면 끝난다. Spark로 짜면 밑줄로 시작하는
파일을 숨김파일로 취급해 스키마 추론이 실패하거나, dedupe 한 번에
shuffle이 필요해지는 등 득보다 실이 크다.

Spark든 pandas든 File Geodatabase(.gdb)를 직접 읽는 방법은 없어서, ogr2ogr
(GDAL CLI, Dockerfile에 gdal-bin으로 설치됨)로 필요한 컬럼 + WKT 지오메트리만
CSV로 평탄화한 뒤 그 CSV를 읽는다.
"""

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

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_silver")

LION_BRONZE_ROOT = BRONZE_DIR / "lion"
DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"
DIM_SEGMENT_STAGING_ROOT = SILVER1_DIR / "_staging" / "dim_segment"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")

LION_COLUMNS = [
    "SegmentID", "Street", "RW_TYPE", "TRUCK_ROUTE_TYPE", "TrafDir",
    "FeatureTyp", "Number_Travel_Lanes", "Number_Total_Lanes",
    "StreetWidth_Min", "StreetWidth_Max", "SHAPE_Length", "LBoro", "NodeIDFrom", "NodeIDTo",
    "POSTED_SPEED",
]

VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
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


def _clean_lion_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """ogr2ogr가 뽑은 평탄 DataFrame을 정제한다. ogr2ogr 의존이 없어 단독으로
    단위 테스트할 수 있다."""

    df = df.copy()

    if "SHAPE" not in df.columns and "WKT" in df.columns:
        df = df.rename(columns={"WKT": "SHAPE"})

    df["RW_TYPE"] = df["RW_TYPE"].str.strip()
    df["TRUCK_ROUTE_TYPE"] = df["TRUCK_ROUTE_TYPE"].str.strip()
    df["Number_Travel_Lanes"] = pd.to_numeric(df["Number_Travel_Lanes"].astype(str).str.strip(), errors="coerce")
    df["SHAPE_Length"] = pd.to_numeric(df["SHAPE_Length"], errors="coerce")
    # 제한속도 미표기 segment가 실측 기준 약 32%라 흔한 케이스다(errors="coerce"로
    # 빈 문자열 -> NaN) - type1 SPEC Estimate 폴백(src/nav_time/gold2.py)이
    # 이 결측을 보고 그 segment는 추정 자체를 건너뛴다.
    df["POSTED_SPEED"] = pd.to_numeric(df["POSTED_SPEED"].astype(str).str.strip(), errors="coerce")
    df["Street"] = df["Street"].apply(clean_street)

    before = len(df)
    df = df.drop_duplicates(subset="SegmentID", keep="first")
    logger.info(f"[lion_silver] dedupe: {before}행 -> {len(df)}행")

    dim_segment = df.rename(
        columns={
            "SegmentID": "segment_id",
            "Street": "street_name",
            "LBoro": "borough_code",
            "SHAPE": "geometry",
            "SHAPE_Length": "length_ft",
            "Number_Travel_Lanes": "lanes_total",
            "NodeIDFrom": "node_from",
            "NodeIDTo": "node_to",
            "POSTED_SPEED": "speed_limit_mph",
        }
    )[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft",
        "lanes_total", "node_from", "node_to",
        "RW_TYPE", "TRUCK_ROUTE_TYPE", "TrafDir", "FeatureTyp", "speed_limit_mph",
    ]]

    return dim_segment


def _find_gdb(version_dir: Path) -> Path:
    gdbs = list(version_dir.rglob("*.gdb"))
    if not gdbs:
        raise FileNotFoundError(f"{version_dir} 안에 .gdb가 없습니다")
    return gdbs[0]


def _stage_gdb_locally(gdb_path, work_dir: Path) -> Path:
    if isinstance(gdb_path, Path):
        return gdb_path

    local_gdb = work_dir / gdb_path.name
    downloaded_path = Path(gdb_path.download_to(local_gdb))

    if not downloaded_path.is_dir() or not any(p.is_file() for p in downloaded_path.rglob("*")):
        raise RuntimeError(f"LION .gdb 로컬 다운로드 검증 실패: {gdb_path}")

    return downloaded_path


def _gdb_to_flat_csv(gdb_path: Path, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ogr2ogr", "-f", "CSV", str(out_path), str(gdb_path), "lion",
        "-select", ",".join(LION_COLUMNS),
        "-lco", "GEOMETRY=AS_WKT",
        "-nlt", "CONVERT_TO_LINEAR",
    ]
    logger.info(f"[lion_silver] ogr2ogr 실행: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"[lion_silver] ogr2ogr 실패: {result.stderr}")
        raise RuntimeError(f"ogr2ogr 변환 실패: {result.stderr}")

    return out_path


def build_dim_segment_staged(
    bronze_version_path: str,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """지정된 Bronze 스냅샷을 정제해 실행별 임시 경로에 저장한다."""

    version_dir = _as_path(bronze_version_path)
    if not (version_dir / "_metadata.txt").exists():
        raise FileNotFoundError(f"완료되지 않은 LION Bronze 스냅샷입니다: {version_dir}")

    gdb_path = _find_gdb(version_dir)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_silver1_", dir=TMP_DIR) as tmp:
        work_dir = Path(tmp)
        local_gdb_path = _stage_gdb_locally(gdb_path, work_dir)
        tmp_csv = _gdb_to_flat_csv(local_gdb_path, work_dir / "lion_flat.csv")
        raw_df = pd.read_csv(tmp_csv, dtype=str, keep_default_na=False)

    dim_segment = _clean_lion_dataframe(raw_df)
    run_id = uuid4().hex
    run_path = _staging_run_path(run_id, staging_root)
    stage_path = run_path / "dim_segment.parquet"
    stage_path.parent.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(stage_path), index=False)

    logger.info(
        "[lion_silver] staging 저장 완료: rows=%s source=%s path=%s",
        len(dim_segment),
        version_dir,
        stage_path,
    )
    return {
        "run_id": run_id,
        "stage_path": str(stage_path),
        "source_version": str(version_dir),
    }


def validate_dim_segment_base(path: str) -> str:
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"
    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    logger.info(f"[lion_silver] dim_segment(Silver1) 검증 통과 ({n}행) -> {path}")
    return path


def validate_staged_dim_segment(
    stage_result: dict,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """임시 산출물의 경로와 데이터 품질을 검증한다."""

    expected_path = (
        _staging_run_path(stage_result["run_id"], staging_root)
        / "dim_segment.parquet"
    )
    if stage_result.get("stage_path") != str(expected_path):
        raise ValueError("예상하지 못한 LION Silver1 staging 경로입니다")
    validate_dim_segment_base(str(expected_path))
    return stage_result


def publish_dim_segment(
    validated_stage: dict,
    output_path=DIM_SEGMENT_BASE_PATH,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """검증을 통과한 임시 산출물만 Silver1 운영 경로에 반영한다."""

    stage_path = (
        _staging_run_path(validated_stage["run_id"], staging_root)
        / "dim_segment.parquet"
    )
    if validated_stage.get("stage_path") != str(stage_path):
        raise ValueError("예상하지 못한 LION Silver1 staging 경로입니다")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(stage_path, Path):
        shutil.copy2(stage_path, output_path)
    else:
        stage_path.copy(output_path)
    if not output_path.exists():
        raise RuntimeError(f"LION Silver1 운영 경로 반영 실패: {output_path}")

    logger.info("[lion_silver] 운영 경로 반영 완료: %s", output_path)
    return {**validated_stage, "output_path": str(output_path)}


def cleanup_dim_segment_staging(
    published_result: dict,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> None:
    """승격이 완료된 실행의 임시 폴더를 정리한다."""

    run_path = _staging_run_path(published_result["run_id"], staging_root)
    if not run_path.exists():
        return
    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("[lion_silver] staging 정리 완료: %s", run_path)
