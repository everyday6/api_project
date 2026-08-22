# Segment Time Pipeline (type1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** NYC DOT 실시간 속도 데이터를 LION 세그먼트에 매핑해서, 세그먼트별 30분 시간대 버킷 평균 통행시간(type1)을 계산해 `SegmentMetricsType1` DynamoDB 테이블에 upsert하는 파이프라인을 만든다.

**Architecture:** Bronze(속도 데이터 수집, Airflow worker, Socrata)까지만 Airflow에서 돌고, Silver1(정제)→Silver2(LION 세그먼트 매핑)→Gold1(필터)→Gold2(버킷 평균+시간 계산+DynamoDB upsert)는 하나의 EMR Serverless Spark job으로 실행한다. 공간 매칭(Silver2)은 이미 검증된 `src/silver2/ticketmaster_lion.py`의 buffer+nearest 패턴을 그대로 재사용하되, 매칭 대상은 Point(venue)가 아니라 LineString(속도 링크)이다 — 매칭 자체는 distinct link(수천 개 규모)에 대해서만 pandas/geopandas로 한 번 계산하고, 그 결과(link_id→segment_id, 작은 테이블)를 훨씬 큰 시계열 속도 읽기값에 Spark 조인으로 펼친다.

**Tech Stack:** pandas/geopandas(공간 매칭), PySpark(집계, EMR Serverless), Socrata(속도 데이터 수집), boto3(DynamoDB 쓰기)

## Global Constraints

- 설계 문서: `docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md`. Foundation 플랜과 segment-length-pipeline 플랜(LION Gold2 `dim_segment.parquet` — `segment_id`, `length_ft`, `is_routable`, `geometry` 컬럼)이 먼저 완료됐다고 가정한다.
- 단순화 가정(설계 문서 3절): 속도 데이터의 양방향/단방향 구분은 무시하고 `SPEED` 컬럼을 그대로 그 세그먼트의 평균속도로 취급한다.
- `time`은 시간대별 반복 패턴(30분 버킷, 최근 14일 롤링 평균)이다 — 특정 날짜의 실시간 스냅샷이 아니다(설계 문서 4절).
- `SPEED` 단위는 mph(NYC DOT 데이터셋 공식 단위)다. `length_ft`(LION, feet)와 결합해 시간(초)을 계산할 때 단위 변환이 필요하다: `시간(초) = (길이_ft / 5280) / 속도_mph * 3600`.
- SPEED가 0 이하인 판독값은 계산에서 제외한다(0으로 나누기 방지, 무의미한 값이므로 무결점 응답 철학과도 맞음).

---

## File Structure

- Modify: `src/common/config.py` — 속도 데이터셋 URL, 공간 매칭 임계값 추가
- Create: `src/speed/__init__.py`, `src/speed/bronze.py` — 속도 데이터 수집(Socrata) + 신규 데이터 확인
- Create: `src/speed/silver1.py` — 속도 데이터 정제(PySpark, 순수 함수)
- Create: `src/silver2/segment_speed_match.py` — link↔segment 공간 매칭(pandas/geopandas, 순수 함수)
- Create: `src/silver2/segment_speed.py` — Silver2 Spark 래퍼(link 값을 segment로 펼침)
- Create: `src/nav_time/__init__.py`, `src/nav_time/gold1.py` — 최근 윈도우+유효 속도 필터(PySpark)
- Create: `src/nav_time/gold2.py` — 버킷 평균+시간 계산+DynamoDB 포맷/upsert(PySpark)
- Create: `spark_jobs/nav_time_job.py` — EMR Serverless 엔트리포인트
- Create: `dags/segment_time_pipeline.py`
- Tests: `tests/speed/`, `tests/silver2/`, `tests/nav_time/`

---

### Task 1: config.py에 속도 데이터셋/공간 매칭 상수 추가

**Files:**
- Modify: `src/common/config.py`

**Interfaces:**
- Produces: `DATASETS["speed"]`(URL 추가), `SPEED_CRS = "EPSG:4326"`, `SPEED_LION_BUFFER_FT`, `SPEED_LION_WARN_DISTANCE_FT`, `SPEED_LION_MAX_DISTANCE_FT`, `MIN_VALID_SPEED_MPH`

- [ ] **Step 1: 상수 추가**

`src/common/config.py`의 `DATASETS` dict에 항목 추가:

```python
DATASETS = {
    "construction": "https://data.cityofnewyork.us/resource/tqtj-sjs8.json",
    "construction_stipulations": "https://data.cityofnewyork.us/resource/gsgx-6efw.json",
    "closure": "https://data.cityofnewyork.us/resource/ezy6-djsf.json",
    "event": "https://nycopendata.socrata.com/resource/tvpp-9vvx.json",
    "parks": "https://data.cityofnewyork.us/resource/enfh-gkve.json",
    # NYC DOT Real-Time Traffic Speed Data
    # https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds/i4gi-tjb9
    "speed": "https://data.cityofnewyork.us/resource/i4gi-tjb9.json",
}
```

파일 끝(EMR Serverless 섹션 뒤)에 추가:

```python
# ==========================
# 속도(speed) - LION 매핑 설정
# ==========================
#
# ticketmaster/gold1.py의 venue-LION 매핑과 동일한 buffer+nearest 패턴을
# 쓴다 — 대상이 Point(venue)가 아니라 LineString(속도 링크)이라는 점만 다르다.

SPEED_CRS = "EPSG:4326"

# 속도 링크 주변 도로 매핑 반경(feet). 도로 링크는 보통 LION 세그먼트 여러
# 개로 쪼개져 있어(하나의 corridor가 여러 블록으로 나뉨), venue보다 좁게
# 잡아도 충분히 겹친다 — 정성적 초안(TODO, 팀 검토 필요).
SPEED_LION_BUFFER_FT = 50

# fallback nearest 매핑 품질 기준.
SPEED_LION_WARN_DISTANCE_FT = 200
SPEED_LION_MAX_DISTANCE_FT = 1000

# 이 미만인 속도 판독값은 계산에서 제외한다(0 또는 비정상적으로 낮은 값 —
# 정차/정지 상태로 잘못 기록된 값과 실제 정체를 구분하기 위한 정성적
# 초안, TODO 팀 검토 필요).
MIN_VALID_SPEED_MPH = 1.0
```

- [ ] **Step 2: 로드 확인**

Run: `python -c "from src.common import config; print(config.DATASETS['speed'], config.SPEED_LION_BUFFER_FT)"`
Expected: `https://data.cityofnewyork.us/resource/i4gi-tjb9.json 50`

- [ ] **Step 3: Commit**

```bash
git add src/common/config.py
git commit -m "feat: 속도 데이터셋/공간 매칭 설정 상수 추가"
```

---

### Task 2: `src/speed/bronze.py` — 속도 데이터 수집

**Files:**
- Create: `src/speed/__init__.py`(빈 파일)
- Create: `src/speed/bronze.py`
- Test: `tests/speed/test_bronze.py`

**Interfaces:**
- Consumes: `common.socrata.fetch_all`, `common.socrata.make_session`, `config.DATASETS`, `config.BRONZE_DIR`
- Produces: `has_new_speed_data(window_start: datetime, window_end: datetime) -> bool`, `collect_speed_window(window_start: datetime, window_end: datetime, bronze_root=BRONZE_ROOT) -> str`(저장 경로)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/speed/__init__.py`(빈 파일) 생성 후 `tests/speed/test_bronze.py`:

```python
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from src.speed import bronze


def test_has_new_speed_data_true_when_count_positive():
    mock_session = MagicMock()

    with patch.object(bronze, "make_session", return_value=mock_session), \
         patch.object(bronze, "_get_count", return_value=42):
        result = bronze.has_new_speed_data(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30)
        )

    assert result is True


def test_has_new_speed_data_false_when_count_zero():
    with patch.object(bronze, "make_session", return_value=MagicMock()), \
         patch.object(bronze, "_get_count", return_value=0):
        result = bronze.has_new_speed_data(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30)
        )

    assert result is False


def test_collect_speed_window_saves_parquet(tmp_path):
    rows = [
        {"link_id": "1", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "speed": "20.0", "data_as_of": "2026-08-21T12:10:00.000"},
    ]

    with patch.object(bronze, "fetch_all", return_value=rows):
        path = bronze.collect_speed_window(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30), bronze_root=tmp_path
        )

    saved = pd.read_parquet(path)
    assert len(saved) == 2
    assert set(saved["link_id"]) == {"1", "2"}


def test_collect_speed_window_empty_result_returns_empty_string(tmp_path):
    with patch.object(bronze, "fetch_all", return_value=[]):
        path = bronze.collect_speed_window(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30), bronze_root=tmp_path
        )

    assert path == ""
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/speed/test_bronze.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.speed.bronze'`

- [ ] **Step 3: `src/speed/bronze.py` 구현**

```python
"""
Bronze 수집: NYC DOT Real-Time Traffic Speed Data

DOT 소스는 5분 간격으로 갱신되지만, 이 DAG는 30분마다 한 번만 폴링해서
지난 30분 범위(data_as_of 기준)를 한 번의 API 호출로 수집한다 — 5분마다
폴링하지 않는 이유는 파이프라인 전체 스케줄(설계 문서 4절, 30분 버킷)과
맞추기 위함이다. 수집된 5분 단위 판독값은 Bronze에 개별 행으로 그대로
저장한다(정제/집계는 Silver1/Gold2에서).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, DATASETS
from src.common.logger import get_logger
from src.common.socrata import fetch_all, make_session

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze")

SPEED_URL = DATASETS["speed"]
BRONZE_ROOT = BRONZE_DIR / "speed"


def _soql_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _get_count(session, window_start: datetime, window_end: datetime) -> int:
    """지정 구간에 존재하는 행 수를 $select=count(*)로 가볍게 확인한다."""

    where = (
        f"data_as_of > '{_soql_timestamp(window_start)}' "
        f"AND data_as_of <= '{_soql_timestamp(window_end)}'"
    )
    response = session.get(
        SPEED_URL, params={"$select": "count(*)", "$where": where}, timeout=30
    )
    response.raise_for_status()
    return int(response.json()[0]["count"])


def has_new_speed_data(window_start: datetime, window_end: datetime) -> bool:
    """지정 구간에 새 판독값이 하나라도 있으면 True. short-circuit 태스크가 쓴다."""

    session = make_session()
    count = _get_count(session, window_start, window_end)

    logger.info(f"[speed_bronze] {window_start}~{window_end} 구간 판독값 count={count}")
    return count > 0


def collect_speed_window(
    window_start: datetime,
    window_end: datetime,
    bronze_root: Path = BRONZE_ROOT,
) -> str:
    """지정 구간의 속도 판독값을 전부 받아 Bronze에 parquet으로 저장한다.

    결과가 0건이면 빈 문자열을 반환한다(정상 케이스 — 상위 DAG가 short-circuit
    으로 이미 걸러내지만, 이 함수 자체도 방어적으로 처리한다).
    """

    where = (
        f"data_as_of > '{_soql_timestamp(window_start)}' "
        f"AND data_as_of <= '{_soql_timestamp(window_end)}'"
    )

    rows = fetch_all(SPEED_URL, where=where, order="data_as_of")

    if not rows:
        logger.info(f"[speed_bronze] {window_start}~{window_end} 구간 결과 없음")
        return ""

    df = pd.DataFrame(rows)

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / f"window_end={window_end.strftime('%Y%m%dT%H%M')}.parquet"
    df.to_parquet(str(out_path), index=False)

    logger.info(f"[speed_bronze] {len(df)}행 저장 -> {out_path}")
    return str(out_path)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/speed/test_bronze.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/speed/__init__.py src/speed/bronze.py tests/speed/
git commit -m "feat: NYC DOT 속도 데이터 Bronze 수집 추가"
```

---

### Task 3: `src/speed/silver1.py` — 속도 데이터 정제 (PySpark, 순수 함수)

**Files:**
- Create: `src/speed/silver1.py`
- Test: `tests/speed/test_silver1.py`

**Interfaces:**
- Produces: `clean_speed_silver1(df: DataFrame) -> DataFrame`(컬럼: `link_id`, `link_points`, `speed`, `observed_at`)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/speed/test_silver1.py`:

```python
import pytest
from pyspark.sql import SparkSession

from src.speed.silver1 import clean_speed_silver1


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("speed_silver1_test").getOrCreate()
    yield session
    session.stop()


def test_clean_renames_and_casts(spark):
    df = spark.createDataFrame([
        {
            "link_id": "123",
            "speed": "35.5",
            "link_points": "40.7,-74.0 40.71,-74.01",
            "data_as_of": "2026-08-21T12:05:00.000",
        }
    ])

    result = clean_speed_silver1(df).collect()

    assert len(result) == 1
    assert result[0]["link_id"] == "123"
    assert result[0]["speed"] == 35.5
    assert result[0]["link_points"] == "40.7,-74.0 40.71,-74.01"


def test_clean_drops_rows_with_missing_speed(spark):
    df = spark.createDataFrame([
        {"link_id": "1", "speed": None, "link_points": "40.7,-74.0 40.71,-74.01", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "speed": "20.0", "link_points": "40.7,-74.0 40.71,-74.01", "data_as_of": "2026-08-21T12:05:00.000"},
    ])

    result = clean_speed_silver1(df).collect()

    assert len(result) == 1
    assert result[0]["link_id"] == "2"


def test_clean_drops_rows_with_missing_link_points(spark):
    df = spark.createDataFrame([
        {"link_id": "1", "speed": "20.0", "link_points": None, "data_as_of": "2026-08-21T12:05:00.000"},
    ])

    result = clean_speed_silver1(df).collect()

    assert len(result) == 0
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/speed/test_silver1.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.speed.silver1'`

- [ ] **Step 3: `src/speed/silver1.py` 구현**

```python
"""
Silver1 변환: 속도 Bronze -> 정제된 판독값

결측치 제거(speed/link_points 없는 행), 필요 컬럼 프루닝, 컬럼명/타입
통일만 한다. LION 세그먼트 매핑(Silver2)이나 시간대 집계(Gold1/Gold2)는
여기서 하지 않는다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


def clean_speed_silver1(df: DataFrame) -> DataFrame:
    """speed/link_points 결측 행을 제거하고 컬럼명·타입을 통일한다."""

    cleaned = (
        df.filter(col("speed").isNotNull() & col("link_points").isNotNull())
        .withColumn("speed", col("speed").cast("double"))
        .withColumn("observed_at", to_timestamp(col("data_as_of")))
        .select("link_id", "link_points", "speed", "observed_at")
    )

    return cleaned
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/speed/test_silver1.py -v`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add src/speed/silver1.py tests/speed/test_silver1.py
git commit -m "feat: 속도 데이터 Silver1 정제 추가"
```

---

### Task 4: `src/silver2/segment_speed_match.py` — link↔segment 공간 매칭 (순수 함수)

**Files:**
- Create: `src/silver2/segment_speed_match.py`
- Test: `tests/silver2/test_segment_speed_match.py`

**Interfaces:**
- Consumes: `config.SPEED_CRS`, `config.LION_CRS`, `config.SPEED_LION_BUFFER_FT`, `config.SPEED_LION_WARN_DISTANCE_FT`, `config.SPEED_LION_MAX_DISTANCE_FT`
- Produces: `parse_link_points(link_points: str) -> LineString | None`, `match_links_to_segments(links_df: pd.DataFrame, dim_segment_df: pd.DataFrame) -> pd.DataFrame`(컬럼: `link_id`, `segment_id`, `distance_ft`, `mapping_method`)

`src/silver2/ticketmaster_lion.py`의 buffer+intersects+nearest-fallback 패턴을 그대로 따른다 — venue(Point) 대신 속도 링크(LineString)를 매칭 대상으로 쓴다. 이 파일은 Spark를 import하지 않아 EMR 없이 로컬에서 바로 단위 테스트할 수 있다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/silver2/__init__.py`(빈 파일) 생성 후 `tests/silver2/test_segment_speed_match.py`:

```python
import pandas as pd

from src.silver2.segment_speed_match import match_links_to_segments, parse_link_points


def test_parse_link_points_builds_linestring():
    line = parse_link_points("40.700,-74.000 40.701,-74.001")

    assert line is not None
    assert list(line.coords) == [(-74.000, 40.700), (-74.001, 40.701)]


def test_parse_link_points_returns_none_for_single_point():
    assert parse_link_points("40.700,-74.000") is None


def test_match_links_to_segments_buffer_match():
    # LION 세그먼트: (0,0)-(100,0) (feet, EPSG:2263 근사) 근처에 겹치는 링크
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-1", "geometry": "LINESTRING (0 0, 100 0)", "is_routable": True},
    ])

    # WGS84 좌표라 실제 buffer 안에 들어오는지는 좌표 변환에 의존하므로,
    # 이 테스트는 변환 파이프라인 전체가 예외 없이 돌고 결과 스키마가
    # 맞는지를 확인한다(실제 좌표 정합성은 통합 테스트/실데이터로 검증).
    links_df = pd.DataFrame([
        {"link_id": "link-1", "link_points": "40.700,-74.000 40.7001,-74.0001"},
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert list(result.columns) == ["link_id", "segment_id", "distance_ft", "mapping_method"]


def test_match_links_to_segments_skips_unparseable_link():
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-1", "geometry": "LINESTRING (0 0, 100 0)", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-bad", "link_points": "40.700,-74.000"},  # 점 하나뿐 -> 파싱 실패
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert result.empty
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/silver2/test_segment_speed_match.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.silver2.segment_speed_match'`

- [ ] **Step 3: `src/silver2/segment_speed_match.py` 구현**

```python
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
    work = dim_segment_df[dim_segment_df["is_routable"] & dim_segment_df["geometry"].notna()].copy()
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/silver2/test_segment_speed_match.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/silver2/segment_speed_match.py tests/silver2/
git commit -m "feat: 속도 링크-LION segment 공간 매칭 추가"
```

---

### Task 5: `src/silver2/segment_speed.py` — Silver2 Spark 래퍼

**Files:**
- Create: `src/silver2/segment_speed.py`
- Test: `tests/silver2/test_segment_speed.py`

**Interfaces:**
- Consumes: `silver2.segment_speed_match.match_links_to_segments`
- Produces: `build_segment_speed_silver2(speed_silver1_df: DataFrame, dim_segment_df: pd.DataFrame) -> DataFrame`(컬럼: `segment_id`, `speed`, `observed_at`)

distinct link만 pandas로 매칭하고, 그 매핑을 원래 크기의 속도 판독값에 Spark 조인으로 펼친다 — "link 값을 segment로 펼침".

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/silver2/test_segment_speed.py`:

```python
from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.silver2.segment_speed import build_segment_speed_silver2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("segment_speed_test").getOrCreate()
    yield session
    session.stop()


def test_build_segment_speed_silver2_expands_link_to_segments(spark, monkeypatch):
    import src.silver2.segment_speed as module

    # link-1이 seg-1, seg-2 두 개에 매핑된다고 가정 -> 판독값 1개가 2행으로 펼쳐져야 함
    monkeypatch.setattr(
        module,
        "match_links_to_segments",
        lambda links_df, dim_segment_df: pd.DataFrame([
            {"link_id": "link-1", "segment_id": "seg-1", "distance_ft": 10.0, "mapping_method": "buffer"},
            {"link_id": "link-1", "segment_id": "seg-2", "distance_ft": 20.0, "mapping_method": "buffer"},
        ]),
    )

    speed_silver1_df = spark.createDataFrame([
        {"link_id": "link-1", "link_points": "40.7,-74.0 40.71,-74.01", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_df = pd.DataFrame([{"segment_id": "seg-1", "geometry": "x", "is_routable": True}])

    result = build_segment_speed_silver2(speed_silver1_df, dim_segment_df).collect()

    assert sorted(r["segment_id"] for r in result) == ["seg-1", "seg-2"]
    assert all(r["speed"] == 30.0 for r in result)


def test_build_segment_speed_silver2_unmatched_link_produces_no_rows(spark, monkeypatch):
    import src.silver2.segment_speed as module

    monkeypatch.setattr(
        module, "match_links_to_segments", lambda links_df, dim_segment_df: pd.DataFrame(
            columns=["link_id", "segment_id", "distance_ft", "mapping_method"]
        ),
    )

    speed_silver1_df = spark.createDataFrame([
        {"link_id": "link-unmatched", "link_points": "40.7,-74.0 40.71,-74.01", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_df = pd.DataFrame([{"segment_id": "seg-1", "geometry": "x", "is_routable": True}])

    result = build_segment_speed_silver2(speed_silver1_df, dim_segment_df).collect()

    assert result == []
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/silver2/test_segment_speed.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.silver2.segment_speed'`

- [ ] **Step 3: `src/silver2/segment_speed.py` 구현**

```python
"""
Silver2 — 속도 링크 판독값을 LION segment로 펼친다.

distinct link(수천 개 규모)만 pandas/geopandas(segment_speed_match)로
매칭하고, 그 결과(작은 매핑 테이블)를 Spark 조인으로 훨씬 큰 시계열
속도 판독값에 펼친다. 한 링크가 여러 segment에 매핑되면 그 판독값도
그만큼 여러 행으로 복제된다(각 segment가 그 시각 그 속도를 관측했다고
취급).
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame

from src.common.logger import get_logger
from src.silver2.segment_speed_match import match_links_to_segments

logger = get_logger(__name__, log_to_file=True, log_file_stem="segment_speed_silver2")


def build_segment_speed_silver2(speed_silver1_df: DataFrame, dim_segment_df: pd.DataFrame) -> DataFrame:
    """속도 Silver1(link 단위)을 segment 단위로 펼친 Silver2를 만든다."""

    spark = speed_silver1_df.sparkSession

    distinct_links = (
        speed_silver1_df.select("link_id", "link_points").distinct().toPandas()
    )

    mapping_pdf = match_links_to_segments(distinct_links, dim_segment_df)

    logger.info(
        f"[segment_speed_silver2] distinct_links={len(distinct_links)} mapped_rows={len(mapping_pdf)}"
    )

    if mapping_pdf.empty:
        return spark.createDataFrame([], schema="segment_id string, speed double, observed_at timestamp")

    mapping_df = spark.createDataFrame(mapping_pdf[["link_id", "segment_id"]])

    return (
        speed_silver1_df.join(mapping_df, on="link_id", how="inner")
        .select("segment_id", "speed", "observed_at")
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/silver2/test_segment_speed.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/silver2/segment_speed.py tests/silver2/test_segment_speed.py
git commit -m "feat: Silver2 속도-segment 펼침(Spark 래퍼) 추가"
```

---

### Task 6: `src/nav_time/gold1.py` — 최근 윈도우 + 유효 속도 필터 (PySpark)

**Files:**
- Create: `src/nav_time/__init__.py`(빈 파일)
- Create: `src/nav_time/gold1.py`
- Test: `tests/nav_time/test_gold1.py`

**Interfaces:**
- Consumes: `config.ROLLING_WINDOW_DAYS`, `config.MIN_VALID_SPEED_MPH`
- Produces: `filter_recent_valid_speed(df: DataFrame, as_of: datetime, window_days: int = ROLLING_WINDOW_DAYS) -> DataFrame`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/nav_time/__init__.py`(빈 파일) 생성 후 `tests/nav_time/test_gold1.py`:

```python
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_recent_valid_speed


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold1_test").getOrCreate()
    yield session
    session.stop()


def test_filter_excludes_old_readings(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 1, 12, 0)},  # 20일 전
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 20, 12, 0)},  # 1일 전
    ])

    result = filter_recent_valid_speed(df, as_of=datetime(2026, 8, 21, 12, 0), window_days=14).collect()

    assert len(result) == 1
    assert result[0]["observed_at"] == datetime(2026, 8, 20, 12, 0)


def test_filter_excludes_zero_or_negative_speed(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 0.0, "observed_at": datetime(2026, 8, 20, 12, 0)},
        {"segment_id": "1", "speed": 25.0, "observed_at": datetime(2026, 8, 20, 12, 0)},
    ])

    result = filter_recent_valid_speed(df, as_of=datetime(2026, 8, 21, 12, 0), window_days=14).collect()

    assert len(result) == 1
    assert result[0]["speed"] == 25.0
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_time/test_gold1.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.nav_time.gold1'`

- [ ] **Step 3: `src/nav_time/gold1.py` 구현**

```python
"""
Gold1 — 최근 N일 윈도우 + 유효 속도만 남긴다.

type1(시간) 버킷 평균 계산의 입력을 좁힌다: 너무 오래된 판독값과
0 이하(또는 비정상적으로 낮은) 속도 판독값은 제외한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.common.config import MIN_VALID_SPEED_MPH, ROLLING_WINDOW_DAYS


def filter_recent_valid_speed(
    df: DataFrame,
    as_of: datetime,
    window_days: int = ROLLING_WINDOW_DAYS,
) -> DataFrame:
    cutoff = as_of - timedelta(days=window_days)

    return df.filter(
        (col("observed_at") >= lit(cutoff)) & (col("speed") >= MIN_VALID_SPEED_MPH)
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_time/test_gold1.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/nav_time/__init__.py src/nav_time/gold1.py tests/nav_time/
git commit -m "feat: nav_time Gold1(최근 윈도우+유효 속도 필터) 추가"
```

---

### Task 7: `src/nav_time/gold2.py` — 버킷 평균 + 시간 계산 + DynamoDB upsert (PySpark)

**Files:**
- Create: `src/nav_time/gold2.py`
- Test: `tests/nav_time/test_gold2.py`

**Interfaces:**
- Consumes: `config.BUCKET_MINUTES`, `config.AVG_SORT_KEY`, `common.dynamodb.batch_write_items`
- Produces: `compute_bucket_key(observed_at_col) -> Column`, `compute_time_seconds(df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame`(컬럼: `segment_id`, `bucket`, `time_seconds`), `to_dynamodb_items(bucket_df: DataFrame) -> list[dict]`(버킷별 값 + 세그먼트별 AVG 둘 다 포함), `write_to_dynamodb(items: list[dict], table_name: str) -> int`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/nav_time/test_gold2.py`:

```python
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.nav_time import gold2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold2_test").getOrCreate()
    yield session
    session.stop()


def test_compute_time_seconds_uses_length_and_speed(spark):
    # 길이 5280ft(1마일)를 30mph로 -> 1/30시간 = 120초
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "1200"
    assert abs(result[0]["time_seconds"] - 120.0) < 0.01


def test_compute_time_seconds_buckets_to_30_minutes(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 47)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["bucket"] == "1230"


def test_to_dynamodb_items_includes_bucket_and_avg(spark):
    bucket_df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0},
    ])

    items = gold2.to_dynamodb_items(bucket_df)

    by_sk = {(i["segment_id"], i["sk"]): i["value"] for i in items}
    assert by_sk[("1", "1200")] == 30
    assert by_sk[("1", "1230")] == 50
    assert by_sk[("1", "AVG")] == 40  # (30+50)/2


def test_write_to_dynamodb_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "sk": "1200", "value": 30}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_dynamodb(items, "SegmentMetricsType1")

    mock_write.assert_called_once_with("SegmentMetricsType1", items)
    assert count == 1
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_time/test_gold2.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.nav_time.gold2'`

- [ ] **Step 3: `src/nav_time/gold2.py` 구현**

```python
"""
Gold2 — type1(시간) 최종 산출물 계산 + DynamoDB 포맷/upsert

30분 버킷별 평균 속도를 계산하고, LION 길이(length_ft)로 나눠 세그먼트별
통행시간(초)을 구한다. 세그먼트 전체 평균(AVG, fallback 2단계)도 같이
계산한다. DynamoDB에는 버킷 값과 AVG를 모두 upsert한다(설계 문서 7절).

단위: SPEED는 mph, length_ft는 feet. 시간(초) = (길이_ft / 5280) / 속도_mph * 3600.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, concat, floor, hour, lpad, minute

from src.common.config import AVG_SORT_KEY, BUCKET_MINUTES
from src.common.dynamodb import batch_write_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_time_gold2")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0


def _bucket_column():
    bucket_minute = floor(minute("observed_at") / BUCKET_MINUTES) * BUCKET_MINUTES
    return concat(
        lpad(hour("observed_at").cast("string"), 2, "0"),
        lpad(bucket_minute.cast("int").cast("string"), 2, "0"),
    )


def compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame:
    """(segment_id, speed, observed_at)를 30분 버킷별 평균 통행시간(초)으로 집계한다."""

    spark = silver2_df.sparkSession
    length_df = spark.createDataFrame(dim_segment_length_df[["segment_id", "length_ft"]])

    bucketed = silver2_df.withColumn("bucket", _bucket_column())

    bucket_avg_speed = (
        bucketed.groupBy("segment_id", "bucket")
        .agg(avg("speed").alias("avg_speed"))
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )


def to_dynamodb_items(bucket_df: DataFrame) -> list[dict]:
    """버킷별 값 + 세그먼트별 평균(AVG)을 DynamoDB 항목 리스트로 변환한다."""

    rows = bucket_df.collect()

    items = [
        {"segment_id": row["segment_id"], "sk": row["bucket"], "value": round(row["time_seconds"])}
        for row in rows
    ]

    avg_df = bucket_df.groupBy("segment_id").agg(avg("time_seconds").alias("avg_time_seconds"))
    for row in avg_df.collect():
        items.append(
            {"segment_id": row["segment_id"], "sk": AVG_SORT_KEY, "value": round(row["avg_time_seconds"])}
        )

    return items


def write_to_dynamodb(items: list[dict], table_name: str) -> int:
    batch_write_items(table_name, items)
    logger.info(f"[nav_time_gold2] DynamoDB upsert 완료: table={table_name} count={len(items)}")
    return len(items)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_time/test_gold2.py -v`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/nav_time/gold2.py tests/nav_time/test_gold2.py
git commit -m "feat: nav_time Gold2(버킷 평균+시간 계산+DynamoDB upsert) 추가"
```

---

### Task 8: `spark_jobs/nav_time_job.py` — EMR Serverless 엔트리포인트

**Files:**
- Create: `spark_jobs/nav_time_job.py`

**Interfaces:**
- 인자: `--speed-bronze-path`(Airflow가 수집한 원본 Bronze parquet 경로, 아직 정제 전), `--dim-segment-path`(LION Gold2, `segment_id`/`geometry`/`is_routable`/`length_ft`), `--as-of`(ISO 타임스탬프), `--dynamodb-table`, `--output-s3`

이 job이 Silver1 정제(`clean_speed_silver1`)부터 직접 수행한다 — Airflow는 원본 Bronze만 넘긴다(설계 문서 8절: "Silver1~Gold2를 EMR Serverless Spark job이 수행").

- [ ] **Step 1: 스크립트 작성**

`spark_jobs/nav_time_job.py`:

```python
"""
EMR Serverless 잡 엔트리포인트 — 속도 Bronze -> type1(시간) DynamoDB upsert

Silver1(정제) -> Silver2(LION 매핑) -> Gold1(필터) -> Gold2(버킷 평균+시간
계산+upsert)를 한 잡 안에서 순서대로 수행한다. Airflow는 원본 Bronze
parquet만 이 job에 넘긴다.

인자:
  --speed-bronze-path : 속도 Bronze parquet 경로(Airflow가 수집한 원본, 정제 전)
  --dim-segment-path   : LION Gold2 dim_segment.parquet 경로
                         (segment_id, geometry, is_routable, length_ft)
  --as-of              : 이 실행의 기준 시각(ISO 8601) — Gold1 롤링 윈도우 계산 기준
  --dynamodb-table      : upsert할 DynamoDB 테이블명
  --output-s3           : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json
from datetime import datetime

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_recent_valid_speed
from src.nav_time.gold2 import compute_time_seconds, to_dynamodb_items, write_to_dynamodb
from src.silver2.segment_speed import build_segment_speed_silver2
from src.speed.silver1 import clean_speed_silver1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed-bronze-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--dynamodb-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nav-time-gold").getOrCreate()

    try:
        bronze_df = spark.read.parquet(args.speed_bronze_path)
        dim_segment_df = pd.read_parquet(args.dim_segment_path)

        speed_silver1_df = clean_speed_silver1(bronze_df)
        silver2_df = build_segment_speed_silver2(speed_silver1_df, dim_segment_df)

        as_of = datetime.fromisoformat(args.as_of)
        gold1_df = filter_recent_valid_speed(silver2_df, as_of=as_of)

        bucket_df = compute_time_seconds(gold1_df, dim_segment_df[["segment_id", "length_ft"]])
        items = to_dynamodb_items(bucket_df)
        count = write_to_dynamodb(items, args.dynamodb_table)
    finally:
        spark.stop()

    S3Path(args.output_s3).write_text(json.dumps({"count": count}))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add spark_jobs/nav_time_job.py
git commit -m "feat: type1(시간) EMR Serverless job 엔트리포인트 추가"
```

---

### Task 9: `dags/segment_time_pipeline.py` — Airflow DAG

**Files:**
- Create: `dags/segment_time_pipeline.py`

**Interfaces:**
- Consumes: `src.speed.bronze.{has_new_speed_data, collect_speed_window}`, `src.lion.gold2.DIM_SEGMENT_PATH`, `src.common.emr_serverless.run_spark_job`, `src.common.config.{PROJECT_ROOT, DYNAMODB_TABLE_TYPE1, EMR_JOBS_DIR}`

- [ ] **Step 1: DAG 작성**

`dags/segment_time_pipeline.py`:

```python
"""
DAG: segment_time_pipeline (type1 — 시간)

NYC DOT 실시간 속도 데이터를 30분마다 수집해서, LION 세그먼트별 30분 버킷
평균 통행시간을 계산해 DynamoDB(SegmentMetricsType1)에 upsert한다(설계
문서 8절).

Bronze(수집)만 Airflow worker에서 돌고, Silver1~Gold2는 하나의 EMR
Serverless Spark job으로 묶어서 제출한다.
"""

import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from src.common.alerts import notify_slack_failure
from src.common.config import DYNAMODB_TABLE_TYPE1, EMR_JOBS_DIR, PROJECT_ROOT
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.speed.bronze import collect_speed_window, has_new_speed_data

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_time_pipeline",
    description="type1(시간) — NYC DOT 속도 데이터를 세그먼트별 통행시간으로 변환",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type1", "time"],
)
def segment_time_pipeline():

    @task.short_circuit
    def check_new_data(data_interval_start=None, data_interval_end=None) -> bool:
        return has_new_speed_data(data_interval_start, data_interval_end)

    @task
    def collect_bronze(data_interval_start=None, data_interval_end=None) -> str:
        return collect_speed_window(data_interval_start, data_interval_end)

    @task
    def submit_nav_time_job(speed_bronze_path: str, logical_date=None) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_job.py",
            entry_point_args=[
                "--speed-bronze-path", speed_bronze_path,
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--as-of", logical_date.isoformat(),
                "--dynamodb-table", DYNAMODB_TABLE_TYPE1,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    new_data = check_new_data()
    bronze_path = collect_bronze()
    bronze_path.set_upstream(new_data)

    submit_nav_time_job(bronze_path)


segment_time_pipeline()
```

- [ ] **Step 2: DAG 파싱 확인**

Run: `python -c "from dags.segment_time_pipeline import segment_time_pipeline"`
Expected: 에러 없이 종료

- [ ] **Step 3: Airflow가 DAG를 정상 인식하는지 확인**

Run: `docker compose exec airflow-scheduler airflow dags list-import-errors`
Expected: `segment_time_pipeline.py` 관련 에러가 없음

- [ ] **Step 4: Commit**

```bash
git add dags/segment_time_pipeline.py
git commit -m "feat: segment_time_pipeline DAG 추가 (type1 시간)"
```

---

## Self-Review

**Spec coverage**: 설계 문서 8절 `segment_time_pipeline`의 5단계(0.신규확인, 1.Bronze, 2.Silver1+Silver2+Gold1+Gold2 EMR job, 3.upsert, 4.실패 알림)를 Task9(0-1단계)/Task3,4,5,6,7(EMR job 내부 단계들)/Task8(job wiring)/Task9(알림, on_failure_callback)로 전부 커버. 4절(time=반복 버킷)은 Task7의 `_bucket_column`(observed_at의 날짜 부분을 버리고 시:분만 사용)으로 반영. 3절 단순화 가정(방향 무시, SPEED=평균속도)은 별도 로직 없이 자연스럽게 반영(방향 관련 컬럼을 아예 다루지 않음).

**Placeholder scan**: 없음. `SPEED_LION_BUFFER_FT` 등 정성적 초안 값에는 TODO 라벨을 남겼으나 실제 동작하는 구체적 값(50ft 등)이라 "미구현"이 아님.

**Type consistency**: `to_dynamodb_items`가 만드는 `{"segment_id", "sk", "value"}`는 Foundation의 `batch_write_items` 시그니처와 일치. `AVG_SORT_KEY`/`BUCKET_MINUTES`/`ROLLING_WINDOW_DAYS`/`MIN_VALID_SPEED_MPH`는 Foundation(Task1)과 이 플랜(Task1)에서 정의된 이름을 그대로 참조. `build_segment_speed_silver2`가 반환하는 컬럼(`segment_id`,`speed`,`observed_at`)은 `compute_time_seconds`가 기대하는 입력과 일치.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-21-segment-time-pipeline.md`. 마지막으로 서빙 API 플랜을 작성합니다.
