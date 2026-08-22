# Segment Length Pipeline (type2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** LION 도로망에서 세그먼트별 길이(type2)를 뽑아 `SegmentMetricsType2` DynamoDB 테이블에 upsert하는 파이프라인을 만든다.

**Architecture:** LION Bronze(기존)→Silver1(복원, ogr2ogr+pandas, Airflow worker)→Gold2-lion(복원, is_routable 계산, Airflow worker) 순으로 LION 자체의 참조 테이블을 만들고, 그 위에 이번 type2 전용 Gold1(필터)+Gold2(DynamoDB 포맷+업서트)를 EMR Serverless Spark job으로 실행한다. LION 파싱(ogr2ogr)은 GDAL CLI 의존이라 Airflow worker(Dockerfile에 이미 gdal-bin 설치됨)에서 돌리고, 순수 표 형태 필터/포맷 연산만 EMR Serverless로 보낸다.

**Tech Stack:** pandas(LION 파싱), PySpark(type2 필터/포맷, EMR Serverless), boto3(DynamoDB 쓰기), Airflow 3.3 TaskFlow

## Global Constraints

- 설계 문서: `docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md`, Foundation 플랜: `docs/superpowers/plans/2026-08-21-segment-metrics-foundation.md` (이 플랜의 모든 태스크는 Foundation 플랜이 먼저 완료됐다고 가정한다 — `src/common/dynamodb.py`, `src/common/emr_serverless.py`, `config.py`의 DynamoDB/EMR 상수)
- type2는 시간에 따라 변하지 않으므로 DynamoDB에는 세그먼트당 항목 1개(`sk="LENGTH"`)만 저장한다 — 버킷 반복 없음
- 방향(양방향/단방향) 구분은 하지 않는다 — 설계 문서 3절의 단순화 가정과 무관하게, type2는 원래도 길이만 다루므로 방향 개념 자체가 없음
- road_class/capacity_per_hour 등 옛 traffic_score 전용 계산은 절대 되살리지 않는다(YAGNI) — 이번 범위는 `segment_id`, `length_ft`, `is_routable`뿐

---

## File Structure

- Create: `src/lion/silver1.py` — LION bronze → dim_segment 기본 컬럼 정제(pandas, ogr2ogr)
- Modify: `src/lion/bronze.py` — 신규 LION 릴리즈 확인 함수 추가
- Create: `src/lion/gold2.py` — dim_segment에 `is_routable` 계산해 붙임(트리밍된 버전, RDS 쓰기 없음)
- Create: `src/nav_length/__init__.py`
- Create: `src/nav_length/gold1.py` — routable + length_ft>0 필터(PySpark)
- Create: `src/nav_length/gold2.py` — DynamoDB 포맷 변환 + upsert(PySpark job 안에서 호출)
- Create: `spark_jobs/nav_length_job.py` — EMR Serverless 엔트리포인트
- Create: `dags/segment_length_pipeline.py`
- Create: `tests/lion/test_silver1.py`, `tests/lion/test_gold2.py`, `tests/nav_length/test_gold1.py`, `tests/nav_length/test_gold2.py`

---

### Task 1: `src/lion/silver1.py` — LION Silver1 복원

**Files:**
- Create: `src/lion/silver1.py`
- Test: `tests/lion/test_silver1.py`

**Interfaces:**
- Consumes: `config.BRONZE_DIR`, `config.SILVER1_DIR`, `config.TMP_DIR`, `common.utils.clean_street`
- Produces: `build_dim_segment_base(bronze_root=LION_BRONZE_ROOT, silver1_root=SILVER1_DIR) -> str`(경로), `validate_dim_segment_base(path: str) -> str`, `_clean_lion_dataframe(df: pd.DataFrame) -> pd.DataFrame`(순수 함수, ogr2ogr 없이 테스트 가능)

ogr2ogr(GDAL CLI, Dockerfile에 이미 설치됨)로 `.gdb`를 CSV로 평탄화한 뒤, 순수 pandas 정제 로직(`_clean_lion_dataframe`)을 분리해서 ogr2ogr 없이도 단위 테스트가 가능하게 한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/lion/test_silver1.py`:

```python
import pandas as pd

from src.lion.silver1 import _clean_lion_dataframe


def _raw_row(**overrides):
    row = {
        "SegmentID": "1",
        "Street": "  WEST   19 STREET  ",
        "RW_TYPE": " 1 ",
        "TRUCK_ROUTE_TYPE": " 2 ",
        "TrafDir": "T",
        "FeatureTyp": "0",
        "Number_Travel_Lanes": " 2 ",
        "SHAPE_Length": "120.5",
        "LBoro": "1",
        "NodeIDFrom": "10",
        "NodeIDTo": "11",
        "SHAPE": "LINESTRING (0 0, 1 1)",
    }
    row.update(overrides)
    return row


def test_clean_lion_dataframe_renames_and_casts_columns():
    df = pd.DataFrame([_raw_row()])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["segment_id"] == "1"
    assert result.iloc[0]["length_ft"] == 120.5
    assert result.iloc[0]["lanes_total"] == 2
    assert result.iloc[0]["borough_code"] == "1"
    assert result.iloc[0]["geometry"] == "LINESTRING (0 0, 1 1)"


def test_clean_lion_dataframe_strips_street_whitespace():
    df = pd.DataFrame([_raw_row(Street="  WEST   19 STREET  ")])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["street_name"] == "WEST 19 STREET"


def test_clean_lion_dataframe_dedupes_by_segment_id():
    df = pd.DataFrame([_raw_row(SegmentID="1"), _raw_row(SegmentID="1")])

    result = _clean_lion_dataframe(df)

    assert len(result) == 1


def test_clean_lion_dataframe_keeps_rw_type_columns_for_gold2():
    df = pd.DataFrame([_raw_row()])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["RW_TYPE"] == "1"
    assert result.iloc[0]["FeatureTyp"] == "0"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/lion/test_silver1.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.lion.silver1'`

- [ ] **Step 3: `src/lion/silver1.py` 구현**

```python
"""
Silver1 변환: LION bronze -> dim_segment(기본 컬럼)

구조적 정제(컬럼명 통일, 타입 캐스팅, 도로명 정규화, SegmentID dedupe)만
한다. is_routable 계산은 src/lion/gold2.py가 이 산출물을 읽어서 한다 —
그 계산에 필요한 원본 코드 컬럼(RW_TYPE, FeatureTyp)은 이름 그대로
통과시켜 둔다.

pandas를 쓰는 이유: LION은 분기 1회 갱신되는 24만 행짜리 참조 테이블이라
이 컴퓨터 한 대의 메모리로 몇 초면 끝난다. Spark로 짜면 밑줄로 시작하는
파일을 숨김파일로 취급해 스키마 추론이 실패하거나, dedupe 한 번에
shuffle이 필요해지는 등 득보다 실이 크다.

Spark든 pandas든 File Geodatabase(.gdb)를 직접 읽는 방법은 없어서, ogr2ogr
(GDAL CLI, Dockerfile에 gdal-bin으로 설치됨)로 필요한 컬럼 + WKT 지오메트리만
CSV로 평탄화한 뒤 그 CSV를 읽는다.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, SILVER1_DIR, TMP_DIR
from src.common.logger import get_logger
from src.common.utils import clean_street

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_silver")

LION_BRONZE_ROOT = BRONZE_DIR / "lion"
DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"

LION_COLUMNS = [
    "SegmentID", "Street", "RW_TYPE", "TRUCK_ROUTE_TYPE", "TrafDir",
    "FeatureTyp", "Number_Travel_Lanes", "Number_Total_Lanes",
    "StreetWidth_Min", "StreetWidth_Max", "SHAPE_Length", "LBoro", "NodeIDFrom", "NodeIDTo",
]

VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


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
        }
    )[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft",
        "lanes_total", "node_from", "node_to",
        "RW_TYPE", "TRUCK_ROUTE_TYPE", "TrafDir", "FeatureTyp",
    ]]

    return dim_segment


def _latest_bronze_version(bronze_root: Path = LION_BRONZE_ROOT) -> Path:
    versions = sorted(bronze_root.glob("version_date=*"))
    if not versions:
        raise FileNotFoundError(f"LION bronze 데이터가 없습니다: {bronze_root}")
    return versions[-1]


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


def build_dim_segment_base(
    bronze_root: Path = LION_BRONZE_ROOT,
    silver1_root: Path = SILVER1_DIR,
) -> str:
    """LION 최신 bronze 스냅샷을 읽어 dim_segment Silver1(기본 컬럼) 테이블을 만든다."""

    version_dir = _latest_bronze_version(bronze_root)
    gdb_path = _find_gdb(version_dir)
    logger.info(f"[lion_silver] 입력 bronze: {gdb_path}")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_silver1_", dir=TMP_DIR) as tmp:
        work_dir = Path(tmp)
        local_gdb_path = _stage_gdb_locally(gdb_path, work_dir)
        tmp_csv = work_dir / "lion_flat.csv"
        _gdb_to_flat_csv(local_gdb_path, tmp_csv)

        raw_df = pd.read_csv(tmp_csv, dtype=str, keep_default_na=False)

    dim_segment = _clean_lion_dataframe(raw_df)

    dim_segment_path = silver1_root / "dim_segment.parquet"
    silver1_root.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(dim_segment_path), index=False)

    logger.info(f"[lion_silver] dim_segment(Silver1) {len(dim_segment)}행 저장 -> {dim_segment_path}")
    return str(dim_segment_path)


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


if __name__ == "__main__":
    out = build_dim_segment_base()
    validate_dim_segment_base(out)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/lion/test_silver1.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/lion/silver1.py tests/lion/test_silver1.py
git commit -m "feat: LION Silver1(dim_segment 기본 컬럼) 정제 복원"
```

---

### Task 2: `src/lion/gold2.py` — is_routable 계산 (트리밍 복원)

**Files:**
- Create: `src/lion/gold2.py`
- Test: `tests/lion/test_gold2.py`

**Interfaces:**
- Consumes: `config.GOLD2_DIR`, `config.SILVER1_DIR`
- Produces: `DIM_SEGMENT_PATH`(경로 상수), `build_dim_segment(dim_segment_base_path=DIM_SEGMENT_BASE_PATH) -> str`, `validate_dim_segment(path: str) -> str`

road_class/capacity_per_hour/is_two_way 등 옛 traffic_score 전용 계산은 가져오지 않는다 — 이번 범위(type2/길이)는 `is_routable`만 필요하다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/lion/test_gold2.py`:

```python
import pandas as pd

from src.lion.gold2 import _compute_is_routable


def test_is_routable_true_for_normal_street():
    df = pd.DataFrame([{"RW_TYPE": "1", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert result.iloc[0] is True or bool(result.iloc[0]) is True


def test_is_routable_false_for_non_routable_rw_type():
    # RW_TYPE=6 (Path/Trail) -> 차량 통행 불가
    df = pd.DataFrame([{"RW_TYPE": "6", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False


def test_is_routable_false_for_non_physical_feature_type():
    # FeatureTyp != "0" -> 비물리적 세그먼트(경계선 등)
    df = pd.DataFrame([{"RW_TYPE": "1", "FeatureTyp": "5"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False


def test_is_routable_false_for_missing_rw_type():
    df = pd.DataFrame([{"RW_TYPE": "", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/lion/test_gold2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.lion.gold2'`

- [ ] **Step 3: `src/lion/gold2.py` 구현**

```python
"""
Gold2 — LION dim_segment에 is_routable 붙이기

src/lion/silver1.py가 만든 dim_segment(기본 컬럼 + 원본 코드 컬럼)를 읽어서
is_routable만 계산해 붙이고 저장한다. length_ft는 이미 Silver1에 있으므로
그대로 통과시킨다 — type2(길이) 소스로 이 파일의 산출물을 그대로 쓴다.

road_class/capacity_per_hour 등은 이번 범위(nav 세그먼트 지표 API)에
필요하지 않아 계산하지 않는다(YAGNI) — 필요해지면 그때 추가한다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.config import GOLD2_DIR, SILVER1_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_gold2")

DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"
DIM_SEGMENT_PATH = GOLD2_DIR / "dim_segment.parquet"

# RW_TYPE(도로유형 코드): 차량이 통행할 수 없는 유형(Boardwalk, Path/Trail,
# Step Street, Driveway, Alley, Unknown, Non-Physical Segment, U-Turn, Ferry Route).
NON_ROUTABLE_RW_TYPES = ["5", "6", "7", "8", "10", "11", "12", "13", "14"]

VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


def _compute_is_routable(df: pd.DataFrame) -> pd.Series:
    """RW_TYPE(차량 통행 불가 유형)과 FeatureTyp(0=물리적 세그먼트)로
    차량이 실제로 지나갈 수 있는 세그먼트인지 판단한다."""

    is_non_routable = (
        df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES)
        | df["RW_TYPE"].isna()
        | (df["RW_TYPE"] == "")
    )
    is_physical = df["FeatureTyp"] == "0"

    return (~is_non_routable) & is_physical


def build_dim_segment(dim_segment_base_path: Path = DIM_SEGMENT_BASE_PATH) -> str:
    """dim_segment(Silver1)를 읽어 is_routable을 계산해 붙인 완성본을 저장한다."""

    df = pd.read_parquet(str(dim_segment_base_path))

    df["is_routable"] = _compute_is_routable(df)

    dim_segment = df[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft",
        "is_routable", "node_from", "node_to",
    ]]

    GOLD2_DIR.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(DIM_SEGMENT_PATH), index=False)

    logger.info(f"[lion_gold2] dim_segment(Gold2) {len(dim_segment)}행 저장 -> {DIM_SEGMENT_PATH}")
    return str(DIM_SEGMENT_PATH)


def validate_dim_segment(path: str) -> str:
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"

    routable_missing_geom = df.loc[df["is_routable"], "geometry"].isna()
    assert not routable_missing_geom.any(), (
        f"is_routable=True인데 geometry가 없는 행 {routable_missing_geom.sum()}개 발견"
    )

    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    logger.info(f"[lion_gold2] dim_segment(Gold2) 검증 통과 ({n}행) -> {path}")
    return path


if __name__ == "__main__":
    out = build_dim_segment()
    validate_dim_segment(out)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/lion/test_gold2.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/lion/gold2.py tests/lion/test_gold2.py
git commit -m "feat: LION Gold2(is_routable) 복원 — traffic_score 전용 계산 제외"
```

---

### Task 3: `src/lion/bronze.py`에 신규 릴리즈 확인 함수 추가

**Files:**
- Modify: `src/lion/bronze.py`

**Interfaces:**
- Produces: `check_new_lion_release(bronze_root=BRONZE_ROOT) -> bool`(새 릴리즈가 있으면 True)

LION ZIP의 `Last-Modified` HTTP 헤더를 마지막 확인 시점과 비교한다. 마커 파일이 없으면(최초 실행) 항상 True를 반환한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/lion/test_bronze.py`(신규 파일):

```python
from unittest.mock import MagicMock, patch

from src.lion import bronze


def test_check_new_lion_release_true_when_no_marker(tmp_path):
    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is True


def test_check_new_lion_release_false_when_unchanged(tmp_path):
    marker_path = tmp_path / "_last_checked_last_modified.txt"
    marker_path.write_text("Wed, 01 Jan 2026 00:00:00 GMT")

    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is False


def test_check_new_lion_release_true_when_changed_and_updates_marker(tmp_path):
    marker_path = tmp_path / "_last_checked_last_modified.txt"
    marker_path.write_text("Wed, 01 Jan 2026 00:00:00 GMT")

    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Thu, 02 Apr 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is True
    assert marker_path.read_text() == "Thu, 02 Apr 2026 00:00:00 GMT"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/lion/test_bronze.py -v`
Expected: FAIL — `AttributeError: module 'src.lion.bronze' has no attribute 'check_new_lion_release'`

- [ ] **Step 3: `src/lion/bronze.py`에 함수 추가**

`src/lion/bronze.py`의 `ingest_lion` 함수 바로 위에 추가(`from pathlib import Path` 등 기존 import는 그대로 유지):

```python
def check_new_lion_release(marker_dir: Path = BRONZE_ROOT) -> bool:
    """LION ZIP의 Last-Modified 헤더가 마지막 확인 시점과 다르면 True를
    반환한다. 마커 파일이 없으면(최초 실행) 항상 True."""

    marker_path = marker_dir / "_last_checked_last_modified.txt"

    resp = requests.head(LION_ZIP_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    current_last_modified = resp.headers.get("Last-Modified", "")

    if not marker_path.exists():
        marker_dir.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(current_last_modified)
        logger.info(f"[lion] 마커 없음(최초 실행) -> 신규 릴리즈로 처리: {current_last_modified}")
        return True

    previous_last_modified = marker_path.read_text()

    if current_last_modified == previous_last_modified:
        logger.info(f"[lion] 신규 릴리즈 없음: {current_last_modified}")
        return False

    marker_path.write_text(current_last_modified)
    logger.info(f"[lion] 신규 릴리즈 감지: {previous_last_modified} -> {current_last_modified}")
    return True
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/lion/test_bronze.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/lion/bronze.py tests/lion/test_bronze.py
git commit -m "feat: LION 신규 릴리즈 확인(short-circuit용) 함수 추가"
```

---

### Task 4: `src/nav_length/gold1.py` — routable + 길이 유효성 필터 (PySpark)

**Files:**
- Create: `src/nav_length/__init__.py` (빈 파일)
- Create: `src/nav_length/gold1.py`
- Test: `tests/nav_length/test_gold1.py`

**Interfaces:**
- Consumes: 없음(순수 Spark DataFrame 변환 함수, PySpark 표준 타입만 사용)
- Produces: `filter_routable_segments(df: DataFrame) -> DataFrame`(컬럼: `segment_id`, `length_ft`만 유지)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/nav_length/__init__.py`(빈 파일) 생성 후 `tests/nav_length/test_gold1.py`:

```python
import pytest
from pyspark.sql import SparkSession

from src.nav_length.gold1 import filter_routable_segments


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold1_test").getOrCreate()
    yield session
    session.stop()


def test_filter_keeps_routable_positive_length(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 120.5, "is_routable": True},
        {"segment_id": "2", "length_ft": 0.0, "is_routable": True},
        {"segment_id": "3", "length_ft": 80.0, "is_routable": False},
    ])

    result = filter_routable_segments(df).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "1"


def test_filter_output_has_only_segment_id_and_length_ft(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 120.5, "is_routable": True, "street_name": "X"},
    ])

    result = filter_routable_segments(df)

    assert sorted(result.columns) == ["length_ft", "segment_id"]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_length/test_gold1.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.nav_length.gold1'`

- [ ] **Step 3: `src/nav_length/gold1.py` 구현**

```python
"""
Gold1 — LION dim_segment 중 실제 서빙 가능한 세그먼트만 남긴다.

type2(길이) 값은 routable하지 않은(차량 통행 불가) 세그먼트나 길이가 0인
세그먼트에는 의미가 없으므로 걸러낸다. EMR Serverless Spark job
(spark_jobs/nav_length_job.py)이 이 함수를 호출한다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_routable_segments(df: DataFrame) -> DataFrame:
    """routable하고 길이가 0보다 큰 세그먼트만 (segment_id, length_ft)로 남긴다."""

    return (
        df.filter(col("is_routable") & (col("length_ft") > 0))
        .select("segment_id", "length_ft")
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_length/test_gold1.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/nav_length/__init__.py src/nav_length/gold1.py tests/nav_length/
git commit -m "feat: nav_length Gold1(routable+길이 유효성 필터) 추가"
```

---

### Task 5: `src/nav_length/gold2.py` — DynamoDB 포맷 변환 + upsert (PySpark)

**Files:**
- Create: `src/nav_length/gold2.py`
- Test: `tests/nav_length/test_gold2.py`

**Interfaces:**
- Consumes: `config.LENGTH_SORT_KEY`, `common.dynamodb.batch_write_items`
- Produces: `to_dynamodb_items(df: DataFrame) -> list[dict]`(순수 함수, `[{"segment_id": str, "sk": "LENGTH", "value": int}, ...]`), `write_to_dynamodb(items: list[dict], table_name: str) -> int`(쓴 항목 수 반환)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/nav_length/test_gold2.py`:

```python
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from src.nav_length import gold2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold2_test").getOrCreate()
    yield session
    session.stop()


def test_to_dynamodb_items_rounds_length_to_int(spark):
    df = spark.createDataFrame([{"segment_id": "1", "length_ft": 120.7}])

    items = gold2.to_dynamodb_items(df)

    assert items == [{"segment_id": "1", "sk": "LENGTH", "value": 121}]


def test_to_dynamodb_items_multiple_rows(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 100.0},
        {"segment_id": "2", "length_ft": 200.0},
    ])

    items = gold2.to_dynamodb_items(df)

    assert len(items) == 2
    assert {"segment_id": "2", "sk": "LENGTH", "value": 200} in items


def test_write_to_dynamodb_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "sk": "LENGTH", "value": 100}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_dynamodb(items, "SegmentMetricsType2")

    mock_write.assert_called_once_with("SegmentMetricsType2", items)
    assert count == 1
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_length/test_gold2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.nav_length.gold2'`

- [ ] **Step 3: `src/nav_length/gold2.py` 구현**

```python
"""
Gold2 — type2(길이) 최종 산출물을 DynamoDB 포맷으로 변환하고 upsert한다.

DynamoDB는 세그먼트당 항목 1개(sk="LENGTH")만 저장한다 — 길이는 시간에
따라 변하지 않으므로 버킷을 반복 저장하지 않는다(설계 문서 6절).
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.common.config import LENGTH_SORT_KEY
from src.common.dynamodb import batch_write_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_length_gold2")


def to_dynamodb_items(df: DataFrame) -> list[dict]:
    """(segment_id, length_ft) Spark DataFrame을 DynamoDB 항목 리스트로 변환한다.

    결과가 작아(세그먼트당 1개, 최대 몇십만 건) 드라이버로 collect해도 안전하다.
    """
    rows = df.select("segment_id", "length_ft").collect()

    return [
        {"segment_id": row["segment_id"], "sk": LENGTH_SORT_KEY, "value": round(row["length_ft"])}
        for row in rows
    ]


def write_to_dynamodb(items: list[dict], table_name: str) -> int:
    """DynamoDB에 upsert하고 쓴 항목 수를 반환한다."""
    batch_write_items(table_name, items)
    logger.info(f"[nav_length_gold2] DynamoDB upsert 완료: table={table_name} count={len(items)}")
    return len(items)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_length/test_gold2.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/nav_length/gold2.py tests/nav_length/test_gold2.py
git commit -m "feat: nav_length Gold2(DynamoDB 포맷+upsert) 추가"
```

---

### Task 6: `spark_jobs/nav_length_job.py` — EMR Serverless 엔트리포인트

**Files:**
- Create: `spark_jobs/nav_length_job.py`
- Create: `spark_jobs/__init__.py`(빈 파일, 없으면 생성)

**Interfaces:**
- Consumes: `src.nav_length.gold1.filter_routable_segments`, `src.nav_length.gold2.to_dynamodb_items`, `src.nav_length.gold2.write_to_dynamodb`
- 인자: `--dim-segment-path`(LION Gold2 dim_segment.parquet 경로), `--dynamodb-table`(테이블명), `--output-s3`(결과 요약 JSON 경로)

이 스크립트는 EMR Serverless에 `--py-files`로 함께 올라가는 `src.zip`을 통해 `src.nav_length.*`를 import한다(emr_serverless.py의 `_upload_src_bundle` 참고).

- [ ] **Step 1: 스크립트 작성**

`spark_jobs/nav_length_job.py`:

```python
"""
EMR Serverless 잡 엔트리포인트 — LION dim_segment -> type2(길이) DynamoDB upsert

인자:
  --dim-segment-path : LION Gold2 dim_segment.parquet 경로 (s3:// 또는 로컬)
  --dynamodb-table    : upsert할 DynamoDB 테이블명
  --output-s3         : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_length.gold1 import filter_routable_segments
from src.nav_length.gold2 import to_dynamodb_items, write_to_dynamodb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--dynamodb-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nav-length-gold").getOrCreate()

    try:
        df = spark.read.parquet(args.dim_segment_path)
        filtered = filter_routable_segments(df)
        items = to_dynamodb_items(filtered)
        count = write_to_dynamodb(items, args.dynamodb_table)
    finally:
        spark.stop()

    S3Path(args.output_s3).write_text(json.dumps({"count": count}))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark_jobs/nav_length_job.py spark_jobs/__init__.py
git commit -m "feat: type2(길이) EMR Serverless job 엔트리포인트 추가"
```

---

### Task 7: `dags/segment_length_pipeline.py` — Airflow DAG

**Files:**
- Create: `dags/segment_length_pipeline.py`

**Interfaces:**
- Consumes: `src.lion.bronze.check_new_lion_release`, `src.lion.bronze.ingest_lion`, `src.lion.silver1.build_dim_segment_base`, `src.lion.silver1.validate_dim_segment_base`, `src.lion.gold2.build_dim_segment`, `src.lion.gold2.validate_dim_segment`, `src.common.emr_serverless.run_spark_job`, `src.common.config.{PROJECT_ROOT, DYNAMODB_TABLE_TYPE2, EMR_JOBS_DIR}`

- [ ] **Step 1: DAG 작성**

`dags/segment_length_pipeline.py`:

```python
"""
DAG: segment_length_pipeline (type2 — 길이)

LION 도로망에서 세그먼트별 길이를 뽑아 DynamoDB(SegmentMetricsType2)에
upsert한다. 6개월 주기(LION 정식 릴리즈 주기)로 스케줄하되, 매번 확인해서
신규 릴리즈가 없으면 나머지 태스크를 건너뛴다(설계 문서 8절).

LION 파싱(ogr2ogr)은 GDAL CLI 의존이라 Airflow worker에서 돌리고, 순수
필터/포맷 연산(Gold1/Gold2)만 EMR Serverless Spark job으로 제출한다.
"""

import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from src.common.alerts import notify_slack_failure
from src.common.config import DYNAMODB_TABLE_TYPE2, EMR_JOBS_DIR, PROJECT_ROOT
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.bronze import check_new_lion_release, ingest_lion
from src.lion.gold2 import build_dim_segment, validate_dim_segment
from src.lion.silver1 import build_dim_segment_base, validate_dim_segment_base

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_length_pipeline",
    description="type2(길이) — LION 세그먼트 길이를 DynamoDB에 upsert",
    schedule="0 5 1 1,7 *",  # 1월/7월 1일 새벽 5시 (LION 반년 릴리즈 주기에 맞춤)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type2", "length"],
)
def segment_length_pipeline():

    @task.short_circuit
    def check_new_release() -> bool:
        return check_new_lion_release()

    @task
    def ingest(version_date: str) -> str:
        return ingest_lion(version_date=version_date)

    @task
    def build_silver1(_bronze_path: str) -> str:
        path = build_dim_segment_base()
        return validate_dim_segment_base(path)

    @task
    def build_gold2_lion(_silver1_path: str) -> str:
        path = build_dim_segment()
        return validate_dim_segment(path)

    @task
    def submit_nav_length_job(dim_segment_path: str) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_length_{run_id}.json"

        run_spark_job(
            job_name=f"nav-length-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_length_job.py",
            entry_point_args=[
                "--dim-segment-path", dim_segment_path,
                "--dynamodb-table", DYNAMODB_TABLE_TYPE2,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    new_release = check_new_release()
    bronze_path = ingest(version_date="{{ ds }}")
    bronze_path.set_upstream(new_release)

    silver1_path = build_silver1(bronze_path)
    gold2_lion_path = build_gold2_lion(silver1_path)
    submit_nav_length_job(gold2_lion_path)


segment_length_pipeline()
```

- [ ] **Step 2: DAG 파싱 확인**

Run: `python -c "from dags.segment_length_pipeline import segment_length_pipeline"`
Expected: 에러 없이 종료 (import 시점에 DAG가 정의되므로 문법/의존성 오류가 있으면 바로 드러남)

- [ ] **Step 3: Airflow가 DAG를 정상 인식하는지 확인**

로컬 Airflow가 떠 있다면:

Run: `docker compose exec airflow-scheduler airflow dags list-import-errors`
Expected: `segment_length_pipeline.py` 관련 에러가 없음

- [ ] **Step 4: Commit**

```bash
git add dags/segment_length_pipeline.py
git commit -m "feat: segment_length_pipeline DAG 추가 (type2 길이)"
```

---

## Self-Review

**Spec coverage**: 설계 문서 8절 `segment_length_pipeline`의 5단계(0.신규확인 short-circuit, 1.Bronze, 2.Silver1+Silver2(스킵)+Gold1+Gold2, 3.DynamoDB upsert)를 Task 3(0단계)/Task1(1단계, 기존 ingest_lion 재사용)/Task1(Silver1)/Task2(is_routable, Silver2 없음을 파일 구조로 명시)/Task4-5(Gold1/Gold2)/Task6-7(제출+upsert)로 전부 커버.

**Placeholder scan**: 없음 — 모든 함수가 실제 로직을 담고 있고, TODO 표시 없음.

**Type consistency**: `to_dynamodb_items`가 만드는 `{"segment_id": str, "sk": "LENGTH", "value": int}`는 Foundation 플랜의 `dynamodb.batch_write_items(table_name, items)` 시그니처(딱 `list[dict]`)와 일치. `LENGTH_SORT_KEY` 상수는 Foundation Task 1에서 정의됨 — 이름 일치 확인됨.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-21-segment-length-pipeline.md`. 이어서 type1(시간) 파이프라인과 서빙 API 플랜을 작성합니다.
