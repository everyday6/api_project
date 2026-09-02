"""
Silver1 변환: LION bronze -> dim_segment(기본 컬럼)

구조적 정제(컬럼명 통일, 타입 캐스팅, 도로명 정규화, SegmentID dedupe)만
한다. 이 산출물이 곧 dim_segment 완성본이다 — 모든 소비자가 이 파일을
그대로 쓴다. POSTED_SPEED(제한속도)는 type1 SPEC Estimate 폴백
(src/nav_time/gold2.py)이 바로 쓸 수 있게 speed_limit_mph로 이름만 바꿔서
통과시킨다.

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
from airflow.exceptions import AirflowSkipException
from cloudpathlib import S3Path

from src.common.config import BRONZE_DIR, SILVER1_DIR, TMP_DIR
from src.common.logger import get_logger
from src.common.suspect import flag_suspect_pandas, log_quality_gate, suspect_ratio
from src.common.utils import clean_street, save_parquet
from src.lion.expectations import SPEED_LIMIT_MAX_MPH, SPEED_LIMIT_MIN_MPH

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

# mark_suspect_rows()가 표시한 의심 행(is_suspect)의 비율이 이 값을 넘으면
# validate_dim_segment_base()가 publish를 차단한다. LION은 기준 데이터라
# 정합성이 가용성에 우선한다(RELIABILITY_PRINCIPLES.md Tier 0-A) - 값 수준
# 이상치가 평소보다 뭉텅이로 늘었다는 건 원천 스키마/파싱이 깨졌을
# 신호이므로, 오염된 dim_segment를 내보내느니 이번 릴리즈를 막는다.
#
# NOTE: placeholder 값이다. 실제 dim_segment 스냅샷으로 baseline 의심
# 비율을 측정한 뒤 (baseline + 여유분) 또는 그 배수로 조정해야 한다 -
# 아직 실측 근거가 없다.
MAX_SUSPECT_RATIO = 0.05

# 같은 SegmentID의 중복 행 중 핵심 필드(geometry/length/node/borough)가 서로
# 다른 "충돌 중복"의 비율이 이 값을 넘으면 _clean_lion_dataframe이 build를
# 실패시킨다. LION 원천은 같은 SegmentID가 여러 행으로 정상적으로 존재하지만
# (실측 ≈24만행 / ≈21.8만 고유) 그 행들은 보통 완전히 동일하다. 핵심 필드가
# 다른 중복은 "어느 게 맞는 값인지" 알 수 없다는 뜻이라 조용히 첫 행을 고르면
# 안 된다.
#
# NOTE: placeholder. 실제 원천으로 conflict 비율 baseline을 측정한 뒤 조정해야
# 한다 - 아직 실측 근거가 없다(정상값은 0에 가까울 것으로 기대).
MAX_DUPLICATE_CONFLICT_RATIO = 0.01

# _profile_duplicates / deterministic dedup이 "이 SegmentID 그룹 안에서 값이
# 갈리면 충돌"으로 보는 컬럼(dedup 시점의 raw 컬럼명). geometry(SHAPE), 길이,
# 양 끝 노드, borough - 전부 downstream(zone 매핑, 길이 서빙, 노드 그래프)이
# 실제로 쓰는 값이다.
_DUP_CONFLICT_COLUMNS = ["SHAPE", "SHAPE_Length", "NodeIDFrom", "NodeIDTo", "LBoro"]


def _as_path(value):
    if isinstance(value, (Path, S3Path)):
        return value
    text = str(value)
    return S3Path(text) if text.startswith("s3://") else Path(text)


def _staging_run_path(run_id: str, staging_root=DIM_SEGMENT_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 LION Silver1 staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


def _profile_duplicates(df: pd.DataFrame) -> dict:
    """dedup 전 df에서 SegmentID 중복을 프로파일링한다.

    - exact 중복: 같은 SegmentID의 행들이 `_DUP_CONFLICT_COLUMNS`까지 전부 동일
    - conflict 중복: 그 핵심 필드가 서로 다름 → "어느 행이 맞는지" 알 수 없음

    반환한 dict는 로그(run metadata)로 남기고, `conflict_ratio`가 임계치를
    넘으면 호출부가 build를 실패시킨다."""
    total = len(df)
    unique_keys = int(df["SegmentID"].nunique())
    dup_mask = df["SegmentID"].duplicated(keep=False)
    dup_keys = int(df.loc[dup_mask, "SegmentID"].nunique())

    conflict_keys = 0
    if dup_keys:
        cols = [c for c in _DUP_CONFLICT_COLUMNS if c in df.columns]
        # 그룹 안에서 어떤 핵심 컬럼이든 서로 다른 값이 2개 이상이면 conflict.
        # dropna=False - NaN 대 실제값도 불일치로 센다.
        per_key = df.loc[dup_mask].groupby("SegmentID")[cols].nunique(dropna=False)
        conflict_keys = int((per_key > 1).any(axis=1).sum())

    return {
        "total_rows": total,
        "unique_keys": unique_keys,
        "duplicate_keys": dup_keys,
        "exact_duplicate_keys": dup_keys - conflict_keys,
        "conflict_duplicate_keys": conflict_keys,
        "conflict_ratio": (conflict_keys / unique_keys) if unique_keys else 0.0,
    }


def _clean_lion_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """ogr2ogr가 뽑은 평탄 DataFrame을 정제한다. ogr2ogr 의존이 없어 단독으로
    단위 테스트할 수 있다.

    SegmentID 중복은 원천의 정상 특성이라 제거하되(≈24만행 → ≈21.8만),
    (1) 핵심 필드가 갈리는 "충돌 중복"이 임계치를 넘으면 실패시키고,
    (2) 남길 행은 정렬 후 첫 행으로 골라 파일/행 순서와 무관하게 재현되게 한다."""

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
    profile = _profile_duplicates(df)
    logger.info("[lion_silver] SegmentID 중복 프로파일: %s", profile)
    log_quality_gate(
        logger,
        domain="lion",
        metric="conflict_ratio",
        value=profile["conflict_ratio"],
        threshold=MAX_DUPLICATE_CONFLICT_RATIO,
        passed=profile["conflict_ratio"] <= MAX_DUPLICATE_CONFLICT_RATIO,
        conflict_duplicate_keys=profile["conflict_duplicate_keys"],
        unique_keys=profile["unique_keys"],
    )
    if profile["conflict_ratio"] > MAX_DUPLICATE_CONFLICT_RATIO:
        raise ValueError(
            f"SegmentID 충돌 중복 비율이 임계치를 초과했습니다: "
            f"{profile['conflict_ratio']:.2%} > {MAX_DUPLICATE_CONFLICT_RATIO:.2%} "
            f"(핵심 필드가 다른 중복 {profile['conflict_duplicate_keys']}건 / "
            f"고유 {profile['unique_keys']}개 - 원천 데이터 확인 필요)"
        )
    # 파일/행 순서와 무관하게 재현 가능하도록 정렬 후 첫 행을 남긴다.
    # 충돌 중복은 위에서 이미 차단됐으므로, 여기서 고르는 건 사실상 동일한
    # 행들 중 하나다(tiebreaker 자체는 임의지만 결정적이면 충분).
    sort_cols = ["SegmentID"] + [c for c in _DUP_CONFLICT_COLUMNS if c in df.columns]
    df = (
        df.sort_values(sort_cols, kind="stable")
        .drop_duplicates(subset="SegmentID", keep="first")
    )
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

    return mark_suspect_rows(dim_segment)


def mark_suspect_rows(df: pd.DataFrame) -> pd.DataFrame:
    """dim_segment의 값 수준 이상치(음수 길이/차선 수, 비현실적 제한속도,
    끊긴 노드 연결)를 행 단위로 판정해 `is_suspect` 컬럼을 추가한 복사본을
    반환한다. src/speed/bronze_validation.py의 동명 함수와 같은 목적·같은
    패턴이다 - 전면적인 quarantine 대신 표시만 남기는 최소 버전.

    lion에는 GX log-only 실행부가 없다(src/lion/expectations.py 참고) -
    구조적 critical 검증은 validate_dim_segment_base()의 raw assert가, 값
    수준 이상치는 이 함수가 담당한다. 제한속도 범위 임계값만 expectations.py에서
    상수로 공유한다(SPEED_LIMIT_MIN_MPH/MAX_MPH).

    speed_limit_mph는 결측이 흔한 정상 상태라(약 32%) null 자체는 잡지
    않고, 값이 있는데 비현실적인 범위일 때만 잡는다 - GX의
    ExpectColumnValuesToBeBetween이 기본적으로 null을 검사에서 제외하는
    동작과 맞춘다. 복사본 생성·bool 확정·컬럼명은 src.common.suspect로 위임한다.
    """
    suspect = pd.Series(False, index=df.index)
    suspect |= df["length_ft"].notna() & (df["length_ft"] < 0)
    suspect |= df["lanes_total"].notna() & (df["lanes_total"] < 0)
    suspect |= df["speed_limit_mph"].notna() & (
        (df["speed_limit_mph"] < SPEED_LIMIT_MIN_MPH)
        | (df["speed_limit_mph"] > SPEED_LIMIT_MAX_MPH)
    )
    suspect |= df["node_from"].isna() | (df["node_from"].astype(str).str.strip() == "")
    suspect |= df["node_to"].isna() | (df["node_to"].astype(str).str.strip() == "")

    return flag_suspect_pandas(df, suspect)


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
    bronze_version_result: dict,
    staging_root=DIM_SEGMENT_STAGING_ROOT,
) -> dict:
    """지정된 Bronze 스냅샷을 정제해 실행별 임시 경로에 저장한다.

    bronze_version_result는 ingest_lion의 반환값(XCom)이다. 원본이 안
    바뀌었으면(changed=False) 재계산할 게 없으니 건너뛴다 -
    AirflowSkipException을 던지면 Airflow 기본 trigger_rule(all_success)에
    따라 뒤따르는 validate_staged_dim_segment/publish_dim_segment/
    cleanup_dim_segment_staging도 자동으로 같이 스킵되고, publish의
    outlet Asset(lion_dim_segment_ready)도 emit되지 않는다 - 그래서
    별도 태스크나 플래그 전파 없이도 downstream 전체가 조용히 스킵된다
    (src/taxi_zone/silver1.py의 동일 패턴 참고)."""

    if not bronze_version_result.get("changed", True):
        raise AirflowSkipException(
            "LION 원본이 안 바뀌어 Silver1 재생성을 건너뜁니다"
        )

    version_dir = _as_path(bronze_version_result["path"])
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
    save_parquet(dim_segment, stage_path.parent, stage_path.name)

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

    # segment_id 유일성은 _clean_lion_dataframe의 deterministic dedup이 보장하고,
    # 충돌 중복(핵심 필드가 갈리는 경우)은 거기서 이미 차단된다 - 여기서
    # is_unique를 다시 검사하는 건 항상 통과하는 죽은 assert였다(dedup 이후라
    # 당연히 유일). 그래서 뺐다.
    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    ratio = suspect_ratio(df)
    log_quality_gate(
        logger,
        domain="lion",
        metric="suspect_ratio",
        value=ratio,
        threshold=MAX_SUSPECT_RATIO,
        passed=ratio <= MAX_SUSPECT_RATIO,
        rows=n,
        path=str(path),
    )
    assert ratio <= MAX_SUSPECT_RATIO, (
        f"의심 행(is_suspect) 비율이 임계치를 초과했습니다: "
        f"{ratio:.1%} > {MAX_SUSPECT_RATIO:.1%} "
        f"(mark_suspect_rows가 표시한 값 수준 이상치가 평소보다 급증 - 원천 데이터 확인 필요)"
    )

    logger.info(
        f"[lion_silver] dim_segment(Silver1) 검증 통과 ({n}행, 의심 비율 {ratio:.1%}) -> {path}"
    )
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
