# Zone 내부 세그먼트 공간 가중치(spatial_weight) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 2016년 하차 위경도 그리드(`temp/bq-results.csv`)로 TLC zone 내부 세그먼트별 상대 공간 밀집도(`spatial_weight`)를 계산하는 정적 참조 테이블을 만들고, `src/tlc/gold.py`의 zone→segment 균등 분배 로직을 이 가중치 기반 분배로 교체한다.

**Architecture:** 새 모듈 `src/mapping/segment_spatial_weight.py` 하나에 Bronze 적재부터 Silver 산출까지 전부 담는다(DAG 없음, Bronze/Silver 별도 모듈 분리 없음 — 재실행 근거 데이터가 없는 정적 스냅샷이라 `tlc/gold.py`의 collect/build 분리 구조가 필요 없다). `map_zone_segment.py`의 STRtree 패턴을 재사용해 grid point → zone 순으로 매칭하고, 세그먼트 매칭은 `src/mapping/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴을 가져와 zone 내부로 한정한 반경(100ft) 이내 세그먼트 전부에 거리 역가중으로 나눠 배분한다(반경 안에 없으면 zone 내 최근접 1개 fallback). 이렇게 만든 `segment_hotspot_count`를 라플라스 스무딩 후 zone 내부 정규화(합=1)해서 `spatial_weight`를 만든다. 마지막에 `src/tlc/gold.py::_expand_zone_to_segment_hour`가 이 가중치를 곱하도록 바꾼다.

**Tech Stack:** pandas, shapely 2.x(STRtree), pyproj(좌표 변환), pytest. geopandas는 쓰지 않는다(`zone_segment.py`와 같은 이유 — 배포 컨테이너 의존성 최소화).

## Global Constraints

- Airflow DAG 연결 없음 — 2016년 데이터는 재수집 근거가 없는 정적 스냅샷.
- `src/mapping/segment_spatial_weight.py` 파일 하나로 구현 (Bronze/Silver 별도 모듈 분리 없음, collect/build 태스크 분리 없음).
- 원본 CSV(`temp/bq-results.csv`)는 `bronze/tlc/hotspot_2016/dropoff_grid.parquet`로 영구 복사 (재현성, `data/`가 gitignore 대상이라 산출물이 날아가도 원본에서 다시 만들 수 있어야 함).
- 좌표 변환: 그리드 포인트(EPSG:4326) → EPSG:2263(`LION_CRS`). Zone 폴리곤과 세그먼트 geometry는 이미 EPSG:2263이라 변환 불필요.
- 세그먼트 매칭은 **같은 zone 안으로 한정** — zone 경계를 넘는 매칭은 금지 (zone별 `spatial_weight` 합이 정확히 1이어야 함).
- 세그먼트 매칭 방식: grid point 반경 `HOTSPOT_SEGMENT_BUFFER_FT`(100ft) 이내 세그먼트 전부에 거리 역가중(`1/(distance+ε)`)으로 `dropoff_count`를 나눠 배분 (`src/mapping/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴 재사용). 반경 안에 하나도 없으면 zone 내 최근접 1개로 fallback.
- 라플라스 스무딩 상수 `α`, buffer 반경, 거리 역가중 epsilon(`ε`)은 모두 정성적 초안(TODO, 팀 검토 필요) — 기본값 `α=1.0`, `HOTSPOT_SEGMENT_BUFFER_FT=100`, `HOTSPOT_INVERSE_DISTANCE_EPSILON_FT=1.0`으로 시작.
- `dropoff_count_raw`(`dim_segment_tlc_volume`)는 이제 정수(zone 총합의 정확한 복사)가 아니라 `zone_total × spatial_weight`이므로 **float(double)** 타입이 된다 (기존 "long"에서 변경).
- 하차(dropoff) 위치만 반영 (기존 `tlc_volume` 컴포넌트와 동일 관례).

---

### Task 1: Bronze 적재 — `ingest_hotspot_grid()`

**Files:**
- Create: `src/mapping/segment_spatial_weight.py`
- Modify: `src/common/config.py` (상수 추가)
- Test: `tests/mapping/test_segment_spatial_weight.py`

**Interfaces:**
- Produces: `HOTSPOT_CSV_SOURCE_PATH: Path`, `BRONZE_HOTSPOT_PATH: Path`, `MAP_SEGMENT_SPATIAL_WEIGHT_PATH: Path` (모듈 상수), `ingest_hotspot_grid(source_csv_path: Path = HOTSPOT_CSV_SOURCE_PATH, bronze_path: Path = BRONZE_HOTSPOT_PATH) -> str`

- [ ] **Step 1: `src/common/config.py`에 상수 추가**

`src/common/config.py`의 "Ticketmaster - LION 매핑 설정" 섹션 바로 뒤에 다음을 추가한다:

```python

# ==========================
# 2016 하차 위경도 Hotspot 설정
# ==========================

# BigQuery로 받은 2016년 하차 위경도 grid(bq-results.csv)의 좌표계.
# TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 위경도 기준으로
# zone 내부 분포를 볼 수 있는 마지막 해 데이터다.
BQ_HOTSPOT_CRS = "EPSG:4326"

# zone 내부 세그먼트별 spatial_weight 계산 시, grid point가 0건 매칭된
# 세그먼트도 완전히 0이 되지 않게 하는 라플라스 스무딩 상수. 정성적 초안이다
# (TODO, 팀 검토 필요) — docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
LAPLACE_SMOOTHING_ALPHA = 1.0

# grid point 하나(8~11m 셀)가 세그먼트에 매칭될 때, 이 반경(feet) 이내 세그먼트
# 전부를 후보로 삼아 거리 역가중으로 나눠 배분한다. venue-도로 매핑에 쓴
# TICKETMASTER_LION_BUFFER_FT(200ft)보다 좁게 잡은 이유는 grid 셀 자체가 훨씬
# 작기 때문이다. 정성적 초안이다(TODO, 팀 검토 필요).
HOTSPOT_SEGMENT_BUFFER_FT = 100

# 반경 안 세그먼트에 거리 역가중(1/(distance+epsilon))을 매길 때, point가 세그먼트
# 위에 정확히 있어 distance=0이 되는 경우의 0-division만 막는 최소 상수. 정성적
# 초안이다(TODO, 팀 검토 필요).
HOTSPOT_INVERSE_DISTANCE_EPSILON_FT = 1.0
```

- [ ] **Step 2: 실패하는 테스트 작성**

`tests/mapping/test_segment_spatial_weight.py` 새로 생성:

```python
import pandas as pd
import pytest

from src.mapping.segment_spatial_weight import ingest_hotspot_grid


def test_ingest_hotspot_grid_copies_columns_and_adds_metadata(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({
        "lat_bin": [40.75, 40.76],
        "lon_bin": [-73.98, -73.97],
        "dropoff_count": [100, 50],
    }).to_csv(source_csv, index=False)

    bronze_path = tmp_path / "bronze" / "dropoff_grid.parquet"
    out_path = ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=bronze_path)

    assert out_path == str(bronze_path)
    df = pd.read_parquet(bronze_path)
    assert len(df) == 2
    assert list(df["lat_bin"]) == [40.75, 40.76]
    assert list(df["dropoff_count"]) == [100, 50]
    assert (df["_source"] == "bq_2016_dropoff_grid").all()
    assert df["_ingested_at"].notna().all()


def test_ingest_hotspot_grid_missing_column_raises(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({"lat_bin": [40.75], "lon_bin": [-73.98]}).to_csv(source_csv, index=False)

    with pytest.raises(ValueError, match="필수 컬럼"):
        ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=tmp_path / "out.parquet")
```

- [ ] **Step 3: 테스트 실행해서 실패 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.mapping.segment_spatial_weight'`

- [ ] **Step 4: 최소 구현 작성**

`src/mapping/segment_spatial_weight.py` 새로 생성:

```python
"""
Silver 매핑: 2016년 하차 위경도 grid -> zone 내부 세그먼트별 공간 가중치(spatial_weight)

TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 이 모듈이 쓰는
2016년 grid(temp/bq-results.csv, BigQuery로 받아온 결과)는 위경도 기준으로
zone 내부 분포를 직접 볼 수 있는 마지막 스냅샷이다. 재수집할 근거 데이터가
없는 정적 값이라 DAG 연결이나 재실행 스케줄은 두지 않는다 — 스크립트로
직접 실행하는 한 번짜리 산출물이다.

`src/mapping/zone_segment.py`의 세그먼트-zone 1:1 매핑에 이어, 그 zone 내부에서
세그먼트별 상대 밀집도를 추가로 산정한다. 배경/설계 근거는
docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.common.config import (
    BQ_HOTSPOT_CRS,
    BRONZE_DIR,
    LAPLACE_SMOOTHING_ALPHA,
    LION_CRS,
    PROJECT_ROOT,
    SILVER_DIR,
)
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH, TAXI_ZONE_SHAPEFILE, _load_zones

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_segment_spatial_weight")

# temp/bq-results.csv는 이 저장소(my-project-new) 바깥, 프로젝트 루트의
# 스크래치 위치에 있다. PROJECT_ROOT(my-project-new) 기준이 아니라 그
# 부모 디렉터리 기준이다.
HOTSPOT_CSV_SOURCE_PATH = PROJECT_ROOT.parent / "temp" / "bq-results.csv"
BRONZE_HOTSPOT_PATH = BRONZE_DIR / "tlc" / "hotspot_2016" / "dropoff_grid.parquet"
MAP_SEGMENT_SPATIAL_WEIGHT_PATH = SILVER_DIR / "map_segment_spatial_weight.parquet"


def ingest_hotspot_grid(
    source_csv_path: Path = HOTSPOT_CSV_SOURCE_PATH,
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
) -> str:
    """2016년 하차 위경도 grid CSV(BigQuery 결과)를 변환 없이 Bronze parquet로 옮긴다.

    `src/taxi_zone/bronze.py`와 동일 관례로 메타데이터 컬럼만 붙인다. 재실행할
    근거 데이터가 없는 정적 스냅샷이라 한 번만 실행하면 된다.
    """
    df = pd.read_csv(source_csv_path)

    required = {"lat_bin", "lon_bin", "dropoff_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[map_segment_spatial_weight] 필수 컬럼 없음: {missing}")

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "bq_2016_dropoff_grid"

    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_path, index=False)

    logger.info(f"[map_segment_spatial_weight] hotspot grid {len(df)}행 저장 완료 -> {bronze_path}")
    return str(bronze_path)
```

- [ ] **Step 5: 테스트 실행해서 통과 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v`
Expected: PASS (2 passed)

- [ ] **Step 6: 커밋**

```bash
git add src/common/config.py src/mapping/segment_spatial_weight.py tests/mapping/test_segment_spatial_weight.py
git commit -m "feat: 2016 하차 위경도 grid Bronze 적재(ingest_hotspot_grid) 추가"
```

---

### Task 2: 좌표 변환 + zone 매칭 — `_points_from_grid()`, `_match_points_to_zone()`

**Files:**
- Modify: `src/mapping/segment_spatial_weight.py`
- Test: `tests/mapping/test_segment_spatial_weight.py`

**Interfaces:**
- Consumes: Task 1의 `logger`, 상수 (`BQ_HOTSPOT_CRS`, `LION_CRS`, `TAXI_ZONE_SHAPEFILE`, `_load_zones`)
- Produces:
  - `_points_from_grid(bronze_df: pd.DataFrame) -> pd.DataFrame` — 입력 컬럼 `lat_bin, lon_bin, dropoff_count`, 출력 컬럼 `geometry(shapely Point, EPSG:2263), dropoff_count`
  - `_match_points_to_zone(points: pd.DataFrame, zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE) -> pd.DataFrame` — 출력 컬럼 `geometry, dropoff_count, zone_id(int64)`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/mapping/test_segment_spatial_weight.py`에 추가:

```python
from pathlib import Path

from shapely.geometry import Point, Polygon

from src.mapping.segment_spatial_weight import _match_points_to_zone, _points_from_grid


def test_points_from_grid_reprojects_to_lion_crs():
    bronze_df = pd.DataFrame({
        "lat_bin": [40.75],
        "lon_bin": [-73.98],
        "dropoff_count": [42],
    })

    result = _points_from_grid(bronze_df)

    assert len(result) == 1
    assert result.iloc[0]["dropoff_count"] == 42
    point = result.iloc[0]["geometry"]
    # pyproj Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)로
    # (-73.98, 40.75)를 직접 변환해 확인한 실측값.
    assert point.x == pytest.approx(989791.457, abs=0.01)
    assert point.y == pytest.approx(212522.519, abs=0.01)


def test_match_points_to_zone_assigns_zone_id(monkeypatch):
    zones = pd.DataFrame({
        "LocationID": [1, 2],
        "borough": ["Manhattan", "Manhattan"],
        "geom": [
            Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),
            Polygon([(20, 0), (30, 0), (30, 10), (20, 10)]),
        ],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    points = pd.DataFrame({
        "geometry": [Point(5, 5), Point(25, 5), Point(100, 100)],  # 마지막은 어느 zone에도 없음
        "dropoff_count": [10, 20, 30],
    })

    result = _match_points_to_zone(points, zone_shapefile_path=Path("unused"))

    assert len(result) == 2  # 매칭 안 된 포인트는 제외
    assert result.set_index("dropoff_count")["zone_id"].to_dict() == {10: 1, 20: 2}
    assert result["zone_id"].dtype == "int64"


def test_match_points_to_zone_logs_unmatched_count(monkeypatch, caplog):
    zones = pd.DataFrame({
        "LocationID": [1],
        "borough": ["Manhattan"],
        "geom": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    points = pd.DataFrame({
        "geometry": [Point(5, 5), Point(100, 100)],
        "dropoff_count": [10, 20],
    })

    with caplog.at_level("WARNING"):
        result = _match_points_to_zone(points, zone_shapefile_path=Path("unused"))

    assert len(result) == 1
    assert any("1건" in rec.message for rec in caplog.records)
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k "points_from_grid or match_points_to_zone"`
Expected: FAIL with `ImportError: cannot import name '_points_from_grid'`

- [ ] **Step 3: 구현 작성**

`src/mapping/segment_spatial_weight.py`의 `ingest_hotspot_grid` 함수 뒤에 추가:

```python
def _points_from_grid(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Bronze grid(lat_bin, lon_bin, dropoff_count, EPSG:4326)를 EPSG:2263 Point로 변환한다."""
    transformer = Transformer.from_crs(BQ_HOTSPOT_CRS, LION_CRS, always_xy=True)
    x, y = transformer.transform(bronze_df["lon_bin"].to_numpy(), bronze_df["lat_bin"].to_numpy())

    result = bronze_df[["dropoff_count"]].copy()
    result["geometry"] = [Point(xi, yi) for xi, yi in zip(x, y)]
    return result


def _match_points_to_zone(
    points: pd.DataFrame,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
) -> pd.DataFrame:
    """grid point(EPSG:2263 Point)를 Taxi Zone 폴리곤에 point-in-polygon으로 매칭한다.

    `src/mapping/zone_segment.py`의 세그먼트-zone 매칭과 동일한 STRtree 패턴이다.
    매칭 안 되는 포인트는 제외하고 건수만 로그로 남긴다.
    """
    zones = _load_zones(zone_shapefile_path)
    tree = STRtree(zones["geom"].tolist())

    zone_ids: list[int | None] = []
    unmatched = 0
    multi_match = 0
    for point in points["geometry"]:
        idxs = tree.query(point, predicate="intersects")
        if len(idxs) == 0:
            unmatched += 1
            zone_ids.append(None)
            continue
        if len(idxs) > 1:
            multi_match += 1
        zone_ids.append(zones.iloc[idxs[0]]["LocationID"])

    result = points.copy()
    result["zone_id"] = zone_ids

    if multi_match:
        logger.warning(f"[map_segment_spatial_weight] grid point가 zone 경계에 걸쳐 2개 이상 매칭 {multi_match}건 (첫 번째로 결정)")
    if unmatched:
        logger.warning(f"[map_segment_spatial_weight] zone을 못 찾은 grid point {unmatched}건 (제외)")

    matched = result.dropna(subset=["zone_id"]).copy()
    matched["zone_id"] = matched["zone_id"].astype("int64")
    return matched[["geometry", "dropoff_count", "zone_id"]]
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k "points_from_grid or match_points_to_zone"`
Expected: PASS (3 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/mapping/segment_spatial_weight.py tests/mapping/test_segment_spatial_weight.py
git commit -m "feat: hotspot grid point 좌표 변환 + zone 매칭 추가"
```

---

### Task 3: zone 내부 반경 + 거리 역가중 세그먼트 매칭 — `_match_points_to_segment()`

**Files:**
- Modify: `src/mapping/segment_spatial_weight.py`
- Test: `tests/mapping/test_segment_spatial_weight.py`

**Interfaces:**
- Consumes: Task 2의 `_match_points_to_zone()` 출력 스키마(`geometry, dropoff_count, zone_id`), `src.common.config`의 `HOTSPOT_SEGMENT_BUFFER_FT`, `HOTSPOT_INVERSE_DISTANCE_EPSILON_FT`
- Produces: `_match_points_to_segment(points_with_zone: pd.DataFrame, map_zone_segment: pd.DataFrame, dim_segment: pd.DataFrame, buffer_ft: float = HOTSPOT_SEGMENT_BUFFER_FT, epsilon_ft: float = HOTSPOT_INVERSE_DISTANCE_EPSILON_FT) -> pd.DataFrame` — 출력 컬럼 `segment_id, dropoff_count(float64)`. `map_zone_segment`는 컬럼 `segment_id, zone_id`, `dim_segment`는 컬럼 `segment_id, geometry`(WKT string)를 가진다고 가정. 한 point가 여러 세그먼트에 배분되면 여러 행으로 나온다(합은 원래 `dropoff_count`와 같음).

세그먼트 매칭 방식(`src/mapping/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴 재사용): grid point 반경 `buffer_ft` 이내 세그먼트가 있으면 거리 역가중(`1/(distance+epsilon_ft)`)으로 나눠 배분하고, 반경 안에 하나도 없으면 zone 내 최근접 세그먼트 1개로 fallback한다(그 경우 `dropoff_count` 전부가 그 세그먼트로 간다).

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/mapping/test_segment_spatial_weight.py`에 추가:

```python
from shapely.geometry import LineString

from src.mapping.segment_spatial_weight import _match_points_to_segment


def test_match_points_to_segment_restricts_to_same_zone():
    # zone 1에 세그먼트 A(x=0)만 있고, zone 2에 세그먼트 C(x=5)가 있다.
    # point(x=4, zone=1)는 실제로는 zone 2의 C(x=5)가 A(x=0)보다 훨씬 가깝지만,
    # zone 경계를 넘어 매칭되면 zone별 spatial_weight 합이 깨지므로 같은
    # zone(1)의 A로만(반경 100ft 안에 A 하나뿐이라 fallback) 매칭돼야 한다.
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "C"],
        "zone_id": [1, 2],
    })
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "C"],
        "geometry": [
            LineString([(0, 0), (0, 10)]).wkt,
            LineString([(5, 0), (5, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(4, 5)],
        "dropoff_count": [77],
        "zone_id": [1],
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "A"
    assert result.iloc[0]["dropoff_count"] == pytest.approx(77.0)


def test_match_points_to_segment_skips_zone_with_no_segments():
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A"],
        "geometry": [LineString([(0, 0), (0, 10)]).wkt],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(1, 1)],
        "dropoff_count": [5],
        "zone_id": [99],  # map_zone_segment에 없는 zone
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)

    assert len(result) == 0


def test_match_points_to_segment_falls_back_to_nearest_when_buffer_empty():
    # 세그먼트 A(x=0)가 point(x=1000)에서 100ft(=buffer_ft) 훨씬 밖에 있다.
    # 반경 안에 후보가 없으므로 zone 내 최근접(유일한 세그먼트 A)로 fallback,
    # dropoff_count 전부가 A로 간다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A"],
        "geometry": [LineString([(0, 0), (0, 10)]).wkt],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(1000, 5)],
        "dropoff_count": [30],
        "zone_id": [1],
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "A"
    assert result.iloc[0]["dropoff_count"] == pytest.approx(30.0)


def test_match_points_to_segment_splits_by_inverse_distance_within_buffer():
    # zone 1에 세그먼트 A(x=0)와 B(x=20)가 있고, point(x=10, y=0)에서 둘 다
    # buffer_ft=100 이내다. A까지 거리=10, B까지 거리=10으로 같으므로
    # 1/(10+eps) 가중치가 같아 절반씩 나뉘어야 한다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(0, -10), (0, 10)]).wkt,
            LineString([(20, -10), (20, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(10, 0)],
        "dropoff_count": [100],
        "zone_id": [1],
    })

    result = _match_points_to_segment(
        points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0, epsilon_ft=1.0,
    )

    by_segment = result.set_index("segment_id")["dropoff_count"]
    assert len(result) == 2
    assert by_segment["A"] == pytest.approx(50.0)
    assert by_segment["B"] == pytest.approx(50.0)
    assert by_segment.sum() == pytest.approx(100.0)  # 원래 dropoff_count 보존


def test_match_points_to_segment_closer_segment_gets_more_share():
    # A까지 거리=5, B까지 거리=45 -> A가 훨씬 더 많이 받아야 한다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(5, -10), (5, 10)]).wkt,
            LineString([(45, -10), (45, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(0, 0)],
        "dropoff_count": [100],
        "zone_id": [1],
    })

    result = _match_points_to_segment(
        points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0, epsilon_ft=1.0,
    )

    by_segment = result.set_index("segment_id")["dropoff_count"]
    # A: 1/(5+1)=0.1667, B: 1/(45+1)=0.02174 -> A share = 0.1667/(0.1667+0.02174) ≈ 0.8846
    assert by_segment["A"] > by_segment["B"]
    assert by_segment["A"] == pytest.approx(100 * (1 / 6) / (1 / 6 + 1 / 46), rel=1e-6)
    assert by_segment.sum() == pytest.approx(100.0)
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k match_points_to_segment`
Expected: FAIL with `ImportError: cannot import name '_match_points_to_segment'`

- [ ] **Step 3: 구현 작성**

`src/mapping/segment_spatial_weight.py`의 최상단 import 블록에서 `from src.common.config import (...)` 부분(Task 1에서 만든 블록)에 `HOTSPOT_SEGMENT_BUFFER_FT`, `HOTSPOT_INVERSE_DISTANCE_EPSILON_FT`를 추가한다:

```python
from src.common.config import (
    BQ_HOTSPOT_CRS,
    BRONZE_DIR,
    HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
    HOTSPOT_SEGMENT_BUFFER_FT,
    LAPLACE_SMOOTHING_ALPHA,
    LION_CRS,
    PROJECT_ROOT,
    SILVER_DIR,
)
```

그리고 `_match_points_to_zone` 함수 뒤에 추가:

```python
def _match_points_to_segment(
    points_with_zone: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
    dim_segment: pd.DataFrame,
    buffer_ft: float = HOTSPOT_SEGMENT_BUFFER_FT,
    epsilon_ft: float = HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
) -> pd.DataFrame:
    """zone_id별로 그룹화해, 그 zone에 속한 세그먼트 중 point 반경 buffer_ft(feet)
    이내 전부에 거리 역가중(1/(distance+epsilon_ft))으로 dropoff_count를 나눠
    배분한다. 반경 안에 세그먼트가 하나도 없으면 zone 내 최근접 세그먼트 1개로
    fallback한다(그때는 dropoff_count 전부가 그 세그먼트로 간다) —
    `src/mapping/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴과 동일하다.

    zone 경계를 넘는 매칭을 막아야 zone 내부 spatial_weight 합이 정확히 1이
    된다. 세그먼트 집계(같은 segment_id로 여러 point가 매칭되는 경우 합산)는
    이 함수의 책임이 아니라 다음 단계(_aggregate_hotspot_counts)에서 한다.
    """
    segments = dim_segment[["segment_id", "geometry"]].merge(
        map_zone_segment[["segment_id", "zone_id"]], on="segment_id", how="inner"
    )
    segments["geom"] = segments["geometry"].apply(wkt.loads)

    matched_rows = []
    for zone_id, zone_points in points_with_zone.groupby("zone_id"):
        zone_segments = segments[segments["zone_id"] == zone_id]
        if zone_segments.empty:
            continue

        geoms = zone_segments["geom"].tolist()
        segment_ids = zone_segments["segment_id"].tolist()
        tree = STRtree(geoms)

        for point, dropoff_count in zip(zone_points["geometry"], zone_points["dropoff_count"]):
            idxs = tree.query(point.buffer(buffer_ft), predicate="intersects")

            if len(idxs) == 0:
                nearest_idx = tree.nearest(point)
                matched_rows.append({
                    "segment_id": segment_ids[nearest_idx],
                    "dropoff_count": float(dropoff_count),
                })
                continue

            distances = np.array([point.distance(geoms[i]) for i in idxs])
            inv_distance = 1.0 / (distances + epsilon_ft)
            shares = inv_distance / inv_distance.sum()

            for idx, share in zip(idxs, shares):
                matched_rows.append({
                    "segment_id": segment_ids[idx],
                    "dropoff_count": float(dropoff_count) * float(share),
                })

    if not matched_rows:
        return pd.DataFrame({"segment_id": pd.Series(dtype="object"), "dropoff_count": pd.Series(dtype="float64")})

    return pd.DataFrame(matched_rows)
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k match_points_to_segment`
Expected: PASS (5 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/mapping/segment_spatial_weight.py tests/mapping/test_segment_spatial_weight.py
git commit -m "feat: zone 내부 반경 + 거리 역가중 세그먼트 매칭 추가"
```

---

### Task 4: 집계 + 라플라스 스무딩/정규화 — `_aggregate_hotspot_counts()`, `_compute_spatial_weight()`

**Files:**
- Modify: `src/mapping/segment_spatial_weight.py`
- Test: `tests/mapping/test_segment_spatial_weight.py`

**Interfaces:**
- Consumes: Task 3의 `_match_points_to_segment()` 출력 (`segment_id, dropoff_count`)
- Produces:
  - `_aggregate_hotspot_counts(matched_points: pd.DataFrame, map_zone_segment: pd.DataFrame) -> pd.DataFrame` — 출력 컬럼 `segment_id, zone_id, segment_hotspot_count(float64)` (Task 3의 거리 역가중 분배 때문에 소수 — 정수만 들어와도 float으로 처리). `map_zone_segment`에 있는 세그먼트 전부(매칭 0건 포함) 포함.
  - `_compute_spatial_weight(df: pd.DataFrame, alpha: float = LAPLACE_SMOOTHING_ALPHA) -> pd.DataFrame` — 입력 컬럼 `segment_id, zone_id, segment_hotspot_count`에 `spatial_weight` 컬럼 추가.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/mapping/test_segment_spatial_weight.py`에 추가:

```python
from src.mapping.segment_spatial_weight import _aggregate_hotspot_counts, _compute_spatial_weight


def test_aggregate_hotspot_counts_fills_unmatched_segments_with_zero():
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
    })
    matched_points = pd.DataFrame({
        "segment_id": ["A", "A", "C"],
        "dropoff_count": [10, 5, 3],
    })

    result = _aggregate_hotspot_counts(matched_points, map_zone_segment)

    counts = result.set_index("segment_id")["segment_hotspot_count"]
    assert counts["A"] == 15
    assert counts["B"] == 0  # 매칭된 grid point 없음
    assert counts["C"] == 3
    assert len(result) == 3


def test_aggregate_hotspot_counts_handles_no_matches_at_all():
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    matched_points = pd.DataFrame({"segment_id": pd.Series(dtype="object"), "dropoff_count": pd.Series(dtype="float64")})

    result = _aggregate_hotspot_counts(matched_points, map_zone_segment)

    assert len(result) == 2
    assert (result["segment_hotspot_count"] == 0).all()


def test_compute_spatial_weight_sums_to_one_per_zone():
    df = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
        "segment_hotspot_count": [90, 0, 5],
    })

    result = _compute_spatial_weight(df, alpha=1.0)

    zone1 = result[result["zone_id"] == 1].set_index("segment_id")["spatial_weight"]
    assert zone1["A"] == pytest.approx(91 / 92)
    assert zone1["B"] == pytest.approx(1 / 92)
    assert zone1.sum() == pytest.approx(1.0)

    zone2 = result[result["zone_id"] == 2]["spatial_weight"]
    assert zone2.iloc[0] == pytest.approx(1.0)  # zone에 세그먼트가 하나뿐이면 무조건 1


def test_compute_spatial_weight_never_fully_zero():
    df = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1], "segment_hotspot_count": [1000, 0]})

    result = _compute_spatial_weight(df, alpha=1.0)

    assert (result["spatial_weight"] > 0).all()
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k "aggregate_hotspot_counts or compute_spatial_weight"`
Expected: FAIL with `ImportError: cannot import name '_aggregate_hotspot_counts'`

- [ ] **Step 3: 구현 작성**

`src/mapping/segment_spatial_weight.py`의 `_match_points_to_segment` 함수 뒤에 추가:

```python
def _aggregate_hotspot_counts(
    matched_points: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """매칭된 grid point의 dropoff_count를 segment_id별로 합산한다.

    map_zone_segment 전체(그 zone에 속한 세그먼트 전부)에 left join해서, 매칭이
    0건인 세그먼트도 segment_hotspot_count=0으로 명시적으로 포함시킨다 — 이래야
    다음 단계(_compute_spatial_weight)의 zone 내부 정규화가 zone에 속한 세그먼트
    전부를 커버한다.
    """
    hotspot_counts = (
        matched_points.groupby("segment_id")["dropoff_count"].sum().rename("segment_hotspot_count")
    )

    result = map_zone_segment[["segment_id", "zone_id"]].merge(
        hotspot_counts, on="segment_id", how="left"
    )
    result["segment_hotspot_count"] = result["segment_hotspot_count"].fillna(0.0).astype("float64")
    return result


def _compute_spatial_weight(
    df: pd.DataFrame,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> pd.DataFrame:
    """zone 내부에서 라플라스 스무딩 후 정규화한다 (zone별 spatial_weight 합 = 1).

    alpha는 정성적 초안이다(TODO, 팀 검토 필요) — 매칭 0건 세그먼트가 완전히
    0이 되지 않게 하는 최소한의 목적만 반영했다.
    """
    result = df.copy()
    result["_smoothed"] = result["segment_hotspot_count"] + alpha
    zone_totals = result.groupby("zone_id")["_smoothed"].transform("sum")
    result["spatial_weight"] = result["_smoothed"] / zone_totals
    return result.drop(columns=["_smoothed"])
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k "aggregate_hotspot_counts or compute_spatial_weight"`
Expected: PASS (4 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/mapping/segment_spatial_weight.py tests/mapping/test_segment_spatial_weight.py
git commit -m "feat: 세그먼트별 hotspot 집계 + 라플라스 스무딩 정규화 추가"
```

---

### Task 5: 오케스트레이션 — `build_map_segment_spatial_weight()`, `validate_map_segment_spatial_weight()`

**Files:**
- Modify: `src/mapping/segment_spatial_weight.py`
- Test: `tests/mapping/test_segment_spatial_weight.py`

**Interfaces:**
- Consumes: Task 1~4의 모든 함수
- Produces:
  - `build_map_segment_spatial_weight(bronze_path=BRONZE_HOTSPOT_PATH, map_zone_segment_path=MAP_ZONE_SEGMENT_PATH, dim_segment_path=DIM_SEGMENT_PATH, zone_shapefile_path=TAXI_ZONE_SHAPEFILE, silver_root=SILVER_DIR, alpha=LAPLACE_SMOOTHING_ALPHA) -> str`
  - `validate_map_segment_spatial_weight(path: str, map_zone_segment_path=MAP_ZONE_SEGMENT_PATH) -> str`
  - 저장 스키마: `segment_id, zone_id, segment_hotspot_count, spatial_weight` (Task 4의 `_compute_spatial_weight` 출력과 동일)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/mapping/test_segment_spatial_weight.py`에 추가:

```python
from src.mapping.segment_spatial_weight import (
    build_map_segment_spatial_weight,
    validate_map_segment_spatial_weight,
)


def test_build_and_validate_map_segment_spatial_weight(tmp_path, monkeypatch):
    zones = pd.DataFrame({
        "LocationID": [1],
        "borough": ["Manhattan"],
        "geom": [Polygon([(980000, 200000), (1000000, 200000), (1000000, 220000), (980000, 220000)])],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    bronze_path = tmp_path / "dropoff_grid.parquet"
    pd.DataFrame({
        "lat_bin": [40.75, 40.76],
        "lon_bin": [-73.98, -73.97],
        "dropoff_count": [100, 10],
    }).to_parquet(bronze_path, index=False)

    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(989780, 212510), (989800, 212530)]).wkt,  # (-73.98, 40.75) 근처 -> point1(100건)
            LineString([(992550, 216150), (992570, 216170)]).wkt,  # (-73.97, 40.76) 근처 -> point2(10건)
        ],
    }).to_parquet(dim_segment_path, index=False)

    out_path = build_map_segment_spatial_weight(
        bronze_path=bronze_path,
        map_zone_segment_path=map_zone_segment_path,
        dim_segment_path=dim_segment_path,
        zone_shapefile_path=Path("unused"),
        silver_root=tmp_path,
        alpha=1.0,
    )
    validated_path = validate_map_segment_spatial_weight(out_path, map_zone_segment_path=map_zone_segment_path)
    assert validated_path == out_path

    df = pd.read_parquet(out_path).set_index("segment_id")
    assert df.loc["A", "segment_hotspot_count"] == 100
    assert df.loc["B", "segment_hotspot_count"] == 10
    assert df.loc["A", "spatial_weight"] == pytest.approx(101 / 112)
    assert df.loc["B", "spatial_weight"] == pytest.approx(11 / 112)


def test_validate_map_segment_spatial_weight_rejects_zone_sum_not_one(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "map_segment_spatial_weight.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B"],
        "zone_id": [1, 1],
        "segment_hotspot_count": [10, 5],
        "spatial_weight": [0.5, 0.6],  # 합이 1이 아님(고장난 데이터를 흉내)
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError, match="합이 1이 아님"):
        validate_map_segment_spatial_weight(str(bad_path), map_zone_segment_path=map_zone_segment_path)


def test_validate_map_segment_spatial_weight_rejects_missing_segment(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "map_segment_spatial_weight.parquet"
    pd.DataFrame({
        "segment_id": ["A"],  # B가 빠짐
        "zone_id": [1],
        "segment_hotspot_count": [10],
        "spatial_weight": [1.0],
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError, match="일치하지 않음"):
        validate_map_segment_spatial_weight(str(bad_path), map_zone_segment_path=map_zone_segment_path)
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v -k "build_and_validate or validate_map_segment_spatial_weight"`
Expected: FAIL with `ImportError: cannot import name 'build_map_segment_spatial_weight'`

- [ ] **Step 3: 구현 작성**

`src/mapping/segment_spatial_weight.py`의 `_compute_spatial_weight` 함수 뒤에 추가:

```python
def build_map_segment_spatial_weight(
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
    silver_root: Path = SILVER_DIR,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> str:
    """2016 hotspot grid Bronze + map_zone_segment + dim_segment로 map_segment_spatial_weight를 만든다."""
    bronze_df = pd.read_parquet(bronze_path, columns=["lat_bin", "lon_bin", "dropoff_count"])
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id"])
    dim_segment = pd.read_parquet(dim_segment_path, columns=["segment_id", "geometry"])

    points = _points_from_grid(bronze_df)
    points_with_zone = _match_points_to_zone(points, zone_shapefile_path=zone_shapefile_path)
    matched_points = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)
    aggregated = _aggregate_hotspot_counts(matched_points, map_zone_segment)
    result = _compute_spatial_weight(aggregated, alpha=alpha)

    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "map_segment_spatial_weight.parquet"
    result.to_parquet(out_path, index=False)

    logger.info(f"[map_segment_spatial_weight] {len(result)}행 저장 -> {out_path}")
    return str(out_path)


def validate_map_segment_spatial_weight(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
) -> str:
    """map_segment_spatial_weight.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id"])

    assert df["segment_id"].is_unique, "segment_id 중복 발견"
    assert set(df["segment_id"]) == set(map_zone_segment["segment_id"]), (
        "map_zone_segment의 세그먼트와 정확히 일치하지 않음"
    )
    assert (df["segment_hotspot_count"] >= 0).all(), "segment_hotspot_count에 음수 있음"
    assert df["spatial_weight"].gt(0).all() and df["spatial_weight"].le(1).all(), (
        "spatial_weight가 (0, 1] 범위를 벗어남"
    )

    zone_sums = df.groupby("zone_id")["spatial_weight"].sum()
    assert np.allclose(zone_sums.to_numpy(), 1.0, atol=1e-9), "zone별 spatial_weight 합이 1이 아님"

    logger.info(f"[map_segment_spatial_weight] 검증 통과 ({len(df)}행, zone {df['zone_id'].nunique()}개)")
    return path


if __name__ == "__main__":
    bronze_out = ingest_hotspot_grid()
    silver_out = build_map_segment_spatial_weight(bronze_path=Path(bronze_out))
    validate_map_segment_spatial_weight(silver_out)
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/mapping/test_segment_spatial_weight.py -v`
Expected: PASS (전체 — 이 시점까지 작성한 테스트 전부 통과, 15개 전후)

- [ ] **Step 5: 커밋**

```bash
git add src/mapping/segment_spatial_weight.py tests/mapping/test_segment_spatial_weight.py
git commit -m "feat: map_segment_spatial_weight 빌드/검증 오케스트레이션 추가"
```

---

### Task 6: `src/tlc/gold.py::_expand_zone_to_segment_hour`를 spatial_weight 기반으로 교체

**Files:**
- Modify: `src/tlc/gold.py:93-118` (`_expand_zone_to_segment_hour` 함수 전체)
- Test: `tests/tlc/test_gold.py:35-68` (관련 3개 테스트)

**Interfaces:**
- Consumes: `src.mapping.segment_spatial_weight.MAP_SEGMENT_SPATIAL_WEIGHT_PATH` (Task 5)
- Produces: `_expand_zone_to_segment_hour(zone_hour_counts: pd.DataFrame, map_zone_segment: pd.DataFrame, map_segment_spatial_weight: pd.DataFrame) -> pd.DataFrame` — 반환 컬럼 `segment_id, hour, dropoff_count_raw(float64)` (기존엔 int64였으나 이제 zone 총합 × spatial_weight라 소수가 됨)

- [ ] **Step 1: 기존 테스트를 새 시그니처/기대값으로 수정**

`tests/tlc/test_gold.py`의 `test_expand_zone_to_segment_hour_fills_missing_with_zero`(라인 35-58)를 다음으로 교체한다:

```python
def test_expand_zone_to_segment_hour_weights_by_spatial_share():
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
    })
    map_segment_spatial_weight = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "spatial_weight": [0.7, 0.3, 1.0],
    })
    zone_hour_counts = pd.DataFrame({
        "zone_id": [1, 2],
        "hour": [8, 8],
        "dropoff_count": [100, 5],
    })

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)

    # 세그먼트 3개 x 24시간
    assert len(result) == 3 * 24
    assert set(result.columns) == {"segment_id", "hour", "dropoff_count_raw"}

    hour8 = result[result["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == pytest.approx(70.0)  # zone 1 총합 100 x weight 0.7
    assert hour8["B"] == pytest.approx(30.0)  # zone 1 총합 100 x weight 0.3
    assert hour8["C"] == pytest.approx(5.0)   # zone에 세그먼트 하나뿐 -> weight 1.0

    hour9 = result[result["hour"] == 9].set_index("segment_id")["dropoff_count_raw"]
    assert hour9["A"] == 0  # 트립이 없던 시간대는 0으로 채움


def test_expand_zone_to_segment_hour_preserves_zone_total():
    # spatial_weight 합이 1이면, 세그먼트별로 나눠 가져도 zone 총합은 그대로 보존돼야 한다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    map_segment_spatial_weight = pd.DataFrame({
        "segment_id": ["A", "B"],
        "spatial_weight": [0.9, 0.1],
    })
    zone_hour_counts = pd.DataFrame({"zone_id": [1], "hour": [8], "dropoff_count": [777]})

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)

    hour8_total = result.loc[result["hour"] == 8, "dropoff_count_raw"].sum()
    assert hour8_total == pytest.approx(777.0)


def test_expand_zone_to_segment_hour_missing_spatial_weight_falls_back_to_one():
    # map_segment_spatial_weight에 없는 세그먼트는 1.0으로 폴백한다 — 조용히
    # 0이 되어 traffic_score에서 사라지는 것보다, 예전 균등분배와 같은 결과를
    # 내는 쪽이 안전하다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    map_segment_spatial_weight = pd.DataFrame({
        "segment_id": pd.Series(dtype="object"),
        "spatial_weight": pd.Series(dtype="float64"),
    })
    zone_hour_counts = pd.DataFrame({"zone_id": [1], "hour": [8], "dropoff_count": [42]})

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)

    hour8 = result[result["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == pytest.approx(42.0)
```

`test_expand_zone_to_segment_hour_every_segment_has_24_hours`(라인 61-67)는 새 파라미터만 추가해서 그대로 유지한다:

```python
def test_expand_zone_to_segment_hour_every_segment_has_24_hours():
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    map_segment_spatial_weight = pd.DataFrame({"segment_id": ["A"], "spatial_weight": [1.0]})
    zone_hour_counts = pd.DataFrame({"zone_id": [], "hour": [], "dropoff_count": []})

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)

    assert sorted(result["hour"].tolist()) == list(range(24))
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/tlc/test_gold.py -v -k expand_zone_to_segment_hour`
Expected: FAIL — `TypeError: _expand_zone_to_segment_hour() takes 2 positional arguments but 3 were given` (기존 함수가 아직 2개 인자만 받음)

- [ ] **Step 3: `src/tlc/gold.py`의 `_expand_zone_to_segment_hour` 교체**

`src/tlc/gold.py`에서 다음 블록(현재 93~118행, `_expand_zone_to_segment_hour` 함수 전체)을:

```python
def _expand_zone_to_segment_hour(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """zone x hour 하차수를 segment x hour로 펼친다.

    같은 zone에 속한 세그먼트는 zone 총합을 그대로 나눠 갖지 않고 동일하게
    받는다(세그먼트 수로 나누지 않음). 매치 안 된 시간대는 0으로 채워서
    세그먼트마다 정확히 24행을 보장한다.
    """

    segment_zone = map_zone_segment[["segment_id", "zone_id"]].copy()
    segment_zone["zone_id"] = segment_zone["zone_id"].astype("int64")

    hours = pd.DataFrame({"hour": HOURS})

    grid = segment_zone.merge(hours, how="cross")

    counts = zone_hour_counts.copy()
    counts["zone_id"] = counts["zone_id"].astype("int64")
    counts["hour"] = counts["hour"].astype("int64")

    merged = grid.merge(counts, on=["zone_id", "hour"], how="left")
    merged["dropoff_count_raw"] = merged["dropoff_count"].fillna(0).astype("int64")

    return merged[["segment_id", "hour", "dropoff_count_raw"]]
```

다음으로 교체한다:

```python
def _expand_zone_to_segment_hour(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
    map_segment_spatial_weight: pd.DataFrame,
) -> pd.DataFrame:
    """zone x hour 하차수를 segment x hour로 펼친다.

    같은 zone에 속한 세그먼트라도 동일하게 나눠 갖지 않고,
    map_segment_spatial_weight의 spatial_weight(zone 내부 상대 밀집도, zone별
    합=1, docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md
    참고)만큼 비례해서 나눠 갖는다. spatial_weight가 없는 세그먼트는 1.0으로
    폴백한다 — 조용히 0이 되어 사라지는 것보다 예전 균등분배와 같은 결과를
    내는 쪽이 안전하다. 매치 안 된 시간대는 0으로 채워서 세그먼트마다 정확히
    24행을 보장한다.
    """

    segment_zone = map_zone_segment[["segment_id", "zone_id"]].copy()
    segment_zone["zone_id"] = segment_zone["zone_id"].astype("int64")

    weights = map_segment_spatial_weight[["segment_id", "spatial_weight"]]
    segment_zone = segment_zone.merge(weights, on="segment_id", how="left")

    missing_weight = segment_zone["spatial_weight"].isna()
    if missing_weight.any():
        logger.warning(
            f"[tlc_gold] map_segment_spatial_weight에 없는 세그먼트 {int(missing_weight.sum())}개, "
            "spatial_weight=1.0으로 폴백"
        )
        segment_zone["spatial_weight"] = segment_zone["spatial_weight"].fillna(1.0)

    hours = pd.DataFrame({"hour": HOURS})

    grid = segment_zone.merge(hours, how="cross")

    counts = zone_hour_counts.copy()
    counts["zone_id"] = counts["zone_id"].astype("int64")
    counts["hour"] = counts["hour"].astype("int64")

    merged = grid.merge(counts, on=["zone_id", "hour"], how="left")
    merged["dropoff_count"] = merged["dropoff_count"].fillna(0)
    merged["dropoff_count_raw"] = merged["dropoff_count"] * merged["spatial_weight"]

    return merged[["segment_id", "hour", "dropoff_count_raw"]]
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/tlc/test_gold.py -v -k expand_zone_to_segment_hour`
Expected: PASS (4 passed)

- [ ] **Step 5: 전체 gold 테스트 실행 (이 시점엔 다른 테스트들이 아직 깨져 있을 수 있음을 확인)**

Run: `pytest tests/tlc/test_gold.py -v`
Expected: `test_build_and_validate_dim_segment_tlc_volume` 등 `build_dim_segment_tlc_volume`을 호출하는 테스트들이 FAIL — Task 7에서 고친다. `_expand_zone_to_segment_hour` 관련 테스트 4개는 PASS.

- [ ] **Step 6: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: _expand_zone_to_segment_hour가 spatial_weight 비례 분배하도록 변경"
```

---

### Task 7: `build_dim_segment_tlc_volume`을 spatial_weight 테이블과 연결 + 문서 정리

**Files:**
- Modify: `src/tlc/gold.py:1-30` (import, `build_dim_segment_tlc_volume` 시그니처), `src/tlc/gold.py:133-174` (`build_dim_segment_tlc_volume` 본문)
- Modify: `tests/tlc/test_gold.py` (`build_dim_segment_tlc_volume`을 호출하는 4개 테스트)
- Modify: `docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md` (스키마 표, dtype 정정 + 상위 문서로 포인터 추가)

**Interfaces:**
- Consumes: Task 5의 `MAP_SEGMENT_SPATIAL_WEIGHT_PATH`, Task 6의 새 `_expand_zone_to_segment_hour` 시그니처
- Produces: `build_dim_segment_tlc_volume(zone_hour_counts, map_zone_segment_path=MAP_ZONE_SEGMENT_PATH, map_segment_spatial_weight_path=MAP_SEGMENT_SPATIAL_WEIGHT_PATH, gold_dir=GOLD_DIR, borough=BOROUGH_EVENT) -> str`

- [ ] **Step 1: 기존 테스트에 spatial_weight fixture 추가 (실패 확인용 선반영)**

`tests/tlc/test_gold.py`의 `test_build_and_validate_dim_segment_tlc_volume`(라인 266-306 부근)에서, `map_zone_segment_path` 생성 직후에 다음을 추가하고, `build_dim_segment_tlc_volume` 호출에 `map_segment_spatial_weight_path=map_segment_spatial_weight_path`를 추가한다:

```python
    map_segment_spatial_weight_path = tmp_path / "map_segment_spatial_weight.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B", "C", "D"],
        "spatial_weight": [1.0, 1.0, 1.0, 1.0],
    }).to_parquet(map_segment_spatial_weight_path, index=False)
```

(weight를 전부 1.0으로 두면 `zone_total × 1.0 = zone_total`이라 기존 균등-복사 기대값(`hour8["A"] == 1`, `hour8["B"] == 1` 등)이 그대로 유지된다 — 새 인자가 실제로 읽히고 merge되는지만 확인하는 게 이 테스트의 목적이다.)

같은 방식으로 나머지 3개 테스트에도 추가한다:
- `test_build_dim_segment_tlc_volume_logs_unmatched_zone_trips`(라인 309-353 부근): `map_zone_segment_path`가 segment `A` 하나뿐이므로 `pd.DataFrame({"segment_id": ["A"], "spatial_weight": [1.0]})`
- `test_validate_dim_segment_tlc_volume_rejects_zero_matching_segments`(라인 375-409 부근): `map_zone_segment_path`가 segment `A, B`이므로 `pd.DataFrame({"segment_id": ["A", "B"], "spatial_weight": [1.0, 1.0]})`
- `test_build_then_query_full_pipeline_seam`(라인 480-573 부근): `map_zone_segment_path`가 segment `A, B, C`이므로 `pd.DataFrame({"segment_id": ["A", "B", "C"], "spatial_weight": [1.0, 1.0, 1.0]})`

각 테스트의 `build_dim_segment_tlc_volume(...)` 호출에 `map_segment_spatial_weight_path=map_segment_spatial_weight_path` 인자를 추가한다. 각 테스트의 나머지 assertion(숫자 기대값 포함)은 전부 그대로 둔다.

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/tlc/test_gold.py -v -k "build_and_validate_dim_segment_tlc_volume or logs_unmatched_zone_trips or rejects_zero_matching_segments or full_pipeline_seam"`
Expected: FAIL — `TypeError: build_dim_segment_tlc_volume() got an unexpected keyword argument 'map_segment_spatial_weight_path'`

- [ ] **Step 3: `src/tlc/gold.py` 수정**

파일 상단 import 블록(현재 12-25행)의:

```python
from src.common.config import BOROUGH_EVENT, GOLD_DIR, SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH
```

를 다음으로 교체:

```python
from src.common.config import BOROUGH_EVENT, GOLD_DIR, SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.segment_spatial_weight import MAP_SEGMENT_SPATIAL_WEIGHT_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH
```

`build_dim_segment_tlc_volume` 함수(현재 133-174행)의 시그니처와 본문 앞부분:

```python
def build_dim_segment_tlc_volume(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    gold_dir: Path = GOLD_DIR,
    borough: str = BOROUGH_EVENT,
) -> str:
    """zone x hour 집계 결과를 받아 dim_segment_tlc_volume.parquet을 만든다.

    무거운 Silver 전체 스캔(collect_zone_hour_counts)은 별도 태스크에서
    이미 끝내고 그 결과를 받는다 — 여기서 실패해도(예: 저장 경로 문제) 그
    스캔을 다시 하지 않아도 된다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold 단계에서만 필터링한다.
    """

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id", "borough"])
    map_zone_segment = map_zone_segment.loc[map_zone_segment["borough"] == borough, ["segment_id", "zone_id"]]
```

를 다음으로 교체:

```python
def build_dim_segment_tlc_volume(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    map_segment_spatial_weight_path: Path = MAP_SEGMENT_SPATIAL_WEIGHT_PATH,
    gold_dir: Path = GOLD_DIR,
    borough: str = BOROUGH_EVENT,
) -> str:
    """zone x hour 집계 결과를 받아 dim_segment_tlc_volume.parquet을 만든다.

    무거운 Silver 전체 스캔(collect_zone_hour_counts)은 별도 태스크에서
    이미 끝내고 그 결과를 받는다 — 여기서 실패해도(예: 저장 경로 문제) 그
    스캔을 다시 하지 않아도 된다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold 단계에서만 필터링한다.

    zone -> segment 분배는 균등 복사가 아니라 map_segment_spatial_weight의
    spatial_weight 비례 분배다 (2026-08-19 개정,
    docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md).
    """

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id", "borough"])
    map_zone_segment = map_zone_segment.loc[map_zone_segment["borough"] == borough, ["segment_id", "zone_id"]]

    map_segment_spatial_weight = pd.read_parquet(
        map_segment_spatial_weight_path, columns=["segment_id", "spatial_weight"]
    )
```

그리고 같은 함수 안, `expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)` 줄을:

```python
    expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)
```

다음으로 교체:

```python
    expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/tlc/test_gold.py -v`
Expected: PASS (전체 — Task 6에서 깨졌던 4개 테스트 포함 전부 통과)

- [ ] **Step 5: 오래된 설계 문서의 스키마/데이터흐름 설명 정정**

`docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md`에서 다음 텍스트(현재 66행 부근, 데이터 흐름 다이어그램):

```
        ▼  map_zone_segment.parquet과 join (zone 총합을 그 zone의 모든 세그먼트에 동일 복사)
```

를 다음으로 교체:

```
        ▼  map_zone_segment.parquet과 join (zone 총합을 spatial_weight 비례로 분배 —
        ▼  2026-08-19 개정, docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고)
```

그리고 스키마 표(현재 86행 부근)의:

```
| `dropoff_count_raw` | long | 세그먼트가 속한 zone의 평일 해당 시간대 총 하차수 (같은 zone의 모든 세그먼트가 동일값) |
```

를 다음으로 교체:

```
| `dropoff_count_raw` | double | 세그먼트가 속한 zone의 평일 해당 시간대 총 하차수 x spatial_weight (2026-08-19 개정 전엔 zone의 모든 세그먼트가 동일값을 받는 long 타입이었음) |
```

그리고 "6. zone → segment 펼치기" 항목(현재 110-113행 부근)의:

```
6. **zone → segment 펼치기**: `map_zone_segment.parquet`(`segment_id`, `zone_id`, `borough`)에서
   먼저 `borough == "Manhattan"`인 세그먼트만 남긴 뒤, `dropoff_location_id == zone_id`로
   join한다. 하나의 zone에 여러 세그먼트가 속하면, zone의 하차수 총합을 그 세그먼트
   전부에 동일하게 복사한다(세그먼트 수로 나누지 않음).
```

를 다음으로 교체:

```
6. **zone → segment 펼치기**: `map_zone_segment.parquet`(`segment_id`, `zone_id`, `borough`)에서
   먼저 `borough == "Manhattan"`인 세그먼트만 남긴 뒤, `dropoff_location_id == zone_id`로
   join한다. 하나의 zone에 여러 세그먼트가 속하면, `map_segment_spatial_weight.parquet`의
   `spatial_weight`(zone 내부 상대 밀집도, zone별 합=1) 비례로 나눠 갖는다 (2026-08-19
   개정 — 이전엔 zone 총합을 세그먼트 전부에 동일하게 복사했다. 설계 근거는
   docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고).
```

- [ ] **Step 6: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md
git commit -m "feat: build_dim_segment_tlc_volume을 map_segment_spatial_weight와 연결"
```

---

## Self-Review 결과

**1. 스펙 커버리지:**
- Bronze 적재(`bq-results.csv` -> `bronze/tlc/hotspot_2016/dropoff_grid.parquet`): Task 1
- 좌표 변환(EPSG:4326 -> EPSG:2263): Task 2
- zone point-in-polygon 매칭(`zone_segment.py` 패턴 재사용): Task 2
- zone 내부로 한정한 반경(100ft) + 거리 역가중 세그먼트 매칭, 반경 밖이면 최근접 1개 fallback(`ticketmaster_lion.py` 패턴 재사용): Task 3
- 라플라스 스무딩 + zone 내 정규화(합=1): Task 4
- 오케스트레이션 + 검증 함수: Task 5
- `src/tlc/gold.py` 수정(`_expand_zone_to_segment_hour`, `build_dim_segment_tlc_volume`): Task 6, 7
- DAG 없음 / 파일 하나로 구현: 전체 Task가 `src/mapping/segment_spatial_weight.py` 한 파일 안에서 이뤄짐, DAG 파일 없음 — 충족.
- 문서 동기화(오래된 스키마 설명 정정): Task 7 Step 5

**2. Placeholder 스캔:** `α`(`LAPLACE_SMOOTHING_ALPHA`), `HOTSPOT_SEGMENT_BUFFER_FT`(100ft), `HOTSPOT_INVERSE_DISTANCE_EPSILON_FT`(1.0ft)를 "정성적 초안(TODO)"로 명시한 것은 설계 문서 자체가 의도적으로 남긴 결정이라 placeholder가 아니다(기본값을 실제로 코드에 반영했고, 테스트도 그 값으로 구체적인 숫자를 검증한다). 그 외 TBD/TODO/"나중에" 문구 없음.

**3. 타입 일관성:** `_points_from_grid`(geometry, dropoff_count) -> `_match_points_to_zone`(geometry, dropoff_count, zone_id) -> `_match_points_to_segment`(segment_id, dropoff_count: float64, 반경 매칭이면 여러 행으로 분수 배분) -> `_aggregate_hotspot_counts`(segment_id, zone_id, segment_hotspot_count: float64) -> `_compute_spatial_weight`(+spatial_weight) 순으로 컬럼명과 dtype이 일관되게 이어진다. `_match_points_to_segment`의 반경+거리역가중 매칭이 `dropoff_count`를 소수로 만들기 때문에, Task 3에서 `_aggregate_hotspot_counts`의 `.astype("int64")`를 `.astype("float64")`로 맞춰 바꿨다(Task 4). `_expand_zone_to_segment_hour`가 받는 `map_segment_spatial_weight`의 컬럼명(`segment_id`, `spatial_weight`)도 `_compute_spatial_weight`/`build_map_segment_spatial_weight`의 출력과 동일하다. `dropoff_count_raw`의 dtype 변경(long -> double)을 Task 6/7 양쪽 코드와 문서에서 일관되게 반영했다.
