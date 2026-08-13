# TLC 세그먼트x시간대 통행량 Gold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** TLC 하차(dropoff) 데이터를 세그먼트x평일시간대(0~23시) 단위로 집계한 Gold 테이블(`dim_segment_tlc_volume.parquet`)을 만들고, 공사 위치 세그먼트 + 인접 3단계 이내 세그먼트에 대해 이 값을 조회하는 함수를 만든다.

**Architecture:** `src/tlc/gold.py` 하나에 순수 pandas 로직(zone→segment 펼치기, 정규화, 인접 탐색, 조회)과 Spark 집계 로직(TLC silver 파일 읽기)을 분리해서 담는다. Spark가 필요한 부분은 `spark: SparkSession`을 인자로 받아 테스트에서 `local[1]` 세션을 주입할 수 있게 하고, 나머지는 순수 함수라 작은 pandas DataFrame으로 바로 유닛테스트한다.

**Tech Stack:** Python 3.11, pandas, pyspark(로컬 `local[1]` 모드로 테스트), pytest

## Global Constraints

- 대상 세그먼트는 `map_zone_segment.parquet`에 있는 **맨해튼(`borough == "Manhattan"`)** 세그먼트만이다 (약 19,574개). 공사 허가 신청 자체가 맨해튼 한정이라 이 범위로 좁힌다. `src/tlc/transform.py`(TLC silver, 팀 공용 코드)는 건드리지 않고, Gold 단계에서만 필터링한다.
- 시간 단위는 평일(월~금) 0~23시, 24개 시간대 고정이다. 주말은 다루지 않는다.
- 정규화는 `dropoff_count_raw`를 전체 (segment_id, hour) 조합 기준 global percentile rank(`rank(pct=True, method="average")`)로 계산한다. 세그먼트별/시간대별로 따로 rank하지 않는다.
- 인접 탐색은 3단계(hop) 고정, 자기 자신(0단계) 포함이다.
- 이번 계획은 `src/scoring/traffic_score.py`, `config/traffic_score_weights.yaml`을 수정하지 않는다.
- 재계산은 항상 전체 재계산이다 (증분 없음).

---

## 파일 구조

- **Create:** `src/tlc/gold.py` — Gold 테이블 빌드/검증/조회 함수 전부
- **Create:** `tests/tlc/test_gold.py` — 유닛 테스트 전부
- **Modify:** `requirements.txt` — `pytest` 추가

기존에 참고할 코드 (그대로 두고 import만 함):
- `src/common/config.py` — `SILVER_DIR`, `TAXI_TYPES`, `BOROUGH_EVENT`("Manhattan")
- `src/common/spark.py` — `get_spark()`
- `src/common/logger.py` — `get_logger()`
- `src/mapping/zone_segment.py` — `MAP_ZONE_SEGMENT_PATH`
- `src/lion/segment_adjacency.py` — `GRAPH_SEGMENT_ADJACENCY_PATH`

---

### Task 1: pytest 설정 + zone→segment 하차수 펼치기

**Files:**
- Modify: `requirements.txt`
- Create: `src/tlc/gold.py`
- Create: `tests/tlc/test_gold.py`

**Interfaces:**
- Produces: `_expand_zone_to_segment_hour(zone_hour_counts: pd.DataFrame, map_zone_segment: pd.DataFrame) -> pd.DataFrame`
  - 입력 `zone_hour_counts` 컬럼: `zone_id`(int), `hour`(int, 0~23), `dropoff_count`(int)
  - 입력 `map_zone_segment` 컬럼: `segment_id`(str), `zone_id`(int) (그 외 컬럼 있어도 무시)
  - 출력 컬럼: `segment_id`, `hour`, `dropoff_count_raw` — 세그먼트마다 정확히 24행, 매치 안 된 시간대는 0

- [ ] **Step 1: requirements.txt에 pytest 추가**

`requirements.txt` 맨 아래에 한 줄 추가:

```
pytest==8.4.2
```

- [ ] **Step 2: 실패하는 테스트 작성**

`tests/tlc/test_gold.py` 새로 생성:

```python
import pandas as pd

from src.tlc.gold import _expand_zone_to_segment_hour


def test_expand_zone_to_segment_hour_fills_missing_with_zero():
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
    })
    zone_hour_counts = pd.DataFrame({
        "zone_id": [1, 2],
        "hour": [8, 8],
        "dropoff_count": [100, 5],
    })

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)

    # 세그먼트 3개 x 24시간
    assert len(result) == 3 * 24
    assert set(result.columns) == {"segment_id", "hour", "dropoff_count_raw"}

    hour8 = result[result["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == 100
    assert hour8["B"] == 100  # 같은 zone(1)이면 zone 총합을 그대로 복사
    assert hour8["C"] == 5

    hour9 = result[result["hour"] == 9].set_index("segment_id")["dropoff_count_raw"]
    assert hour9["A"] == 0  # 트립이 없던 시간대는 0으로 채움


def test_expand_zone_to_segment_hour_every_segment_has_24_hours():
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    zone_hour_counts = pd.DataFrame({"zone_id": [], "hour": [], "dropoff_count": []})

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)

    assert sorted(result["hour"].tolist()) == list(range(24))
```

- [ ] **Step 3: 테스트 실행해서 실패 확인**

저장소 루트(`DE-Project/`)에서 실행:

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ModuleNotFoundError: No module named 'src.tlc.gold'` 로 FAIL

- [ ] **Step 4: 최소 구현 작성**

`src/tlc/gold.py` 새로 생성:

```python
"""
TLC Gold — 세그먼트x평일시간대 통행량

TLC 하차(dropoff) 데이터를 "택시 수요"가 아니라 "일반적인 도로 교통량 프록시"로
간주하고, 세그먼트별로 평일 0~23시 각 시간대에 상대적으로 얼마나 붐비는지를
나타내는 Gold 테이블(dim_segment_tlc_volume)을 만든다.

자세한 배경은 docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md
참고.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

from src.common.config import SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")

DIM_SEGMENT_TLC_VOLUME_PATH = SILVER_DIR / "dim_segment_tlc_volume.parquet"

HOURS = list(range(24))
DEFAULT_HOPS = 3


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

- [ ] **Step 5: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 2개 테스트 모두 PASS

- [ ] **Step 6: 커밋**

```bash
git add requirements.txt src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: TLC zone-hour 하차수를 segment-hour로 펼치는 로직 추가"
```

---

### Task 2: percentile rank 정규화

**Files:**
- Modify: `src/tlc/gold.py`
- Modify: `tests/tlc/test_gold.py`

**Interfaces:**
- Consumes: Task 1의 `_expand_zone_to_segment_hour()` 출력 스키마(`segment_id, hour, dropoff_count_raw`)
- Produces: `_normalize_tlc_volume(df: pd.DataFrame) -> pd.DataFrame` — 입력에 `tlc_volume`(float, 0~1) 컬럼을 추가해서 리턴

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_gold.py`에 추가:

```python
from src.tlc.gold import _normalize_tlc_volume


def test_normalize_tlc_volume_percentile_rank():
    df = pd.DataFrame({
        "segment_id": ["A", "B", "C", "D", "E"],
        "hour": [0, 0, 0, 0, 0],
        "dropoff_count_raw": [0, 0, 5, 20, 100],
    })

    result = _normalize_tlc_volume(df)

    values = result.set_index("segment_id")["tlc_volume"]
    assert values["A"] == 0.3
    assert values["B"] == 0.3  # 동점(0)은 평균 등수를 받음
    assert values["C"] == 0.6
    assert values["D"] == 0.8
    assert values["E"] == 1.0


def test_normalize_tlc_volume_keeps_original_columns():
    df = pd.DataFrame({
        "segment_id": ["A", "B"],
        "hour": [0, 1],
        "dropoff_count_raw": [1, 2],
    })

    result = _normalize_tlc_volume(df)

    assert list(result.columns) == ["segment_id", "hour", "dropoff_count_raw", "tlc_volume"]
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ImportError: cannot import name '_normalize_tlc_volume'` 로 FAIL

- [ ] **Step 3: 구현 작성**

`src/tlc/gold.py`에 `_expand_zone_to_segment_hour` 함수 뒤에 추가:

```python
def _normalize_tlc_volume(df: pd.DataFrame) -> pd.DataFrame:
    """dropoff_count_raw를 전체 (segment_id, hour) 조합 기준 global percentile
    rank(0~1)로 정규화한다. dim_segment_traffic_score_v0의 demand_raw(중심성)를
    만들 때 쓴 방식과 동일하다 — 세그먼트/시간대별로 따로 rank하지 않고 전부
    하나로 묶어서 비교한다.
    """

    result = df.copy()
    result["tlc_volume"] = result["dropoff_count_raw"].rank(pct=True, method="average")
    return result
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 4개 테스트 모두 PASS

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: TLC 하차수 percentile rank 정규화 추가"
```

---

### Task 3: TLC silver 파일에서 zone x hour 하차수 집계 (Spark)

**Files:**
- Modify: `src/tlc/gold.py`
- Modify: `tests/tlc/test_gold.py`

**Interfaces:**
- Produces: `_read_zone_hour_counts(spark: SparkSession, silver_dir: Path = SILVER_DIR, taxi_types: list[str] = TAXI_TYPES) -> pd.DataFrame`
  - 출력 컬럼: `zone_id`(int64), `hour`(int64, 0~23), `dropoff_count`(int64)
  - Task 1의 `_expand_zone_to_segment_hour()`가 그대로 입력으로 받을 수 있는 스키마

**참고:** 이 테스트는 실제 spark-master 클러스터가 필요 없다. `local[1]` 모드로 별도
SparkSession을 띄워서 테스트한다 (운영 코드의 `get_spark()`는 이 함수 안에서 호출하지
않고, 호출하는 쪽에서 세션을 주입한다).

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_gold.py` 맨 위에 fixture와 import 추가, 테스트 함수 추가:

```python
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.tlc.gold import _read_zone_hour_counts


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tlc_gold_test").getOrCreate()
    yield session
    session.stop()


def _write_tlc_silver_fixture(base_dir, taxi_type, month, rows):
    """base_dir/{taxi_type}_tripdata_{month}/data.parquet 형태로 TLC silver 픽스처를 만든다."""
    out_dir = base_dir / f"{taxi_type}_tripdata_{month}"
    out_dir.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(out_dir / "data.parquet", index=False)


def test_read_zone_hour_counts_filters_weekday_and_counts(tmp_path, spark):
    # 2024-01-01(월)은 포함, 2024-01-06(토)은 제외되어야 한다.
    rows = [
        {
            "pickup_datetime": datetime(2024, 1, 1, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 8, 30),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 1, 8, 10),
            "dropoff_datetime": datetime(2024, 1, 1, 8, 45),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 2.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 6, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 6, 8, 30),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
    ]
    _write_tlc_silver_fixture(tmp_path, "yellow", "2024-01", rows)

    result = _read_zone_hour_counts(spark, silver_dir=tmp_path, taxi_types=["yellow"])

    assert len(result) == 1
    row = result.iloc[0]
    assert row["zone_id"] == 5
    assert row["hour"] == 8
    assert row["dropoff_count"] == 2  # 월요일 2건만 카운트, 토요일 제외


def test_read_zone_hour_counts_reads_multiple_taxi_types(tmp_path, spark):
    _write_tlc_silver_fixture(tmp_path, "yellow", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 2, 9, 0),
        "dropoff_datetime": datetime(2024, 1, 2, 9, 15),
        "pickup_location_id": 1,
        "dropoff_location_id": 7,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])
    _write_tlc_silver_fixture(tmp_path, "green", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 2, 9, 5),
        "dropoff_datetime": datetime(2024, 1, 2, 9, 20),
        "pickup_location_id": 2,
        "dropoff_location_id": 7,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])

    result = _read_zone_hour_counts(spark, silver_dir=tmp_path, taxi_types=["yellow", "green"])

    assert len(result) == 1
    row = result.iloc[0]
    assert row["zone_id"] == 7
    assert row["hour"] == 9
    assert row["dropoff_count"] == 2  # yellow 1건 + green 1건
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ImportError: cannot import name '_read_zone_hour_counts'` 로 FAIL

- [ ] **Step 3: 구현 작성**

`src/tlc/gold.py` 상단 import에 추가:

```python
from pyspark.sql.functions import col, dayofweek, hour as hour_of_day
```

`_expand_zone_to_segment_hour` 함수 앞에 추가:

```python
def _read_zone_hour_counts(
    spark: SparkSession,
    silver_dir: Path = SILVER_DIR,
    taxi_types: list[str] = TAXI_TYPES,
) -> pd.DataFrame:
    """TLC silver 파일 전부를 읽어 평일(월~금) 기준 zone x hour 하차수를 센다.

    매번 그 시점에 존재하는 파일 전부를 다시 읽어 처음부터 계산한다(전체
    재계산, 증분 아님). group by count는 파티션별 부분 집계 후 작은 결과만
    합치는 구조라 원본 규모(3년치, 약 140개 파일)와 무관하게 메모리 사용량이
    작다.
    """

    paths = [
        str(path)
        for taxi_type in taxi_types
        for path in sorted(silver_dir.glob(f"{taxi_type}_tripdata_*"))
    ]
    if not paths:
        raise FileNotFoundError(f"TLC silver 파일을 찾을 수 없습니다: {silver_dir}")

    logger.info(f"[tlc_gold] TLC silver 파일 {len(paths)}개 읽기 시작")

    df = spark.read.parquet(*paths).select("dropoff_datetime", "dropoff_location_id")

    # Spark의 dayofweek: 일요일=1 ~ 토요일=7. 평일(월~금) = 2~6.
    weekday = df.filter(dayofweek(col("dropoff_datetime")).between(2, 6))

    counted = (
        weekday
        .withColumn("hour", hour_of_day(col("dropoff_datetime")))
        .groupBy(col("dropoff_location_id").alias("zone_id"), "hour")
        .count()
        .withColumnRenamed("count", "dropoff_count")
    )

    result = counted.toPandas()
    result["zone_id"] = result["zone_id"].astype("int64")
    result["hour"] = result["hour"].astype("int64")
    result["dropoff_count"] = result["dropoff_count"].astype("int64")

    logger.info(f"[tlc_gold] zone x hour 집계 완료: {len(result)}행")
    return result
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 6개 테스트 모두 PASS (local Spark 세션 기동에 몇 초 걸릴 수 있음)

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: TLC silver에서 평일 zone x hour 하차수 집계 추가"
```

---

### Task 4: build_dim_segment_tlc_volume + 검증

**Files:**
- Modify: `src/tlc/gold.py`
- Modify: `tests/tlc/test_gold.py`

**Interfaces:**
- Consumes: `_read_zone_hour_counts()`(Task 3), `_expand_zone_to_segment_hour()`(Task 1), `_normalize_tlc_volume()`(Task 2)
- Produces:
  - `build_dim_segment_tlc_volume(spark: SparkSession, map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH, silver_dir: Path = SILVER_DIR, taxi_types: list[str] = TAXI_TYPES, borough: str = BOROUGH_EVENT) -> str` — parquet 저장 경로(str) 리턴
  - `validate_dim_segment_tlc_volume(path: str, map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH, borough: str = BOROUGH_EVENT) -> str` — 검증 통과 시 같은 path 리턴, 실패 시 AssertionError

**대상 지역**: 공사 허가 신청은 맨해튼 한정이라, `map_zone_segment`의 `borough` 컬럼으로
맨해튼 세그먼트만 걸러서 쓴다 (`borough` 값은 실제로 `"Manhattan"` — `src/common/config.py`의
`BOROUGH_EVENT`와 동일한 표기라 그 상수를 그대로 재사용한다). TLC silver(`src/tlc/transform.py`,
팀 공용 코드)는 건드리지 않고, 이 Gold 단계에서만 필터링한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_gold.py`에 추가:

```python
from src.tlc.gold import build_dim_segment_tlc_volume, validate_dim_segment_tlc_volume


def test_build_and_validate_dim_segment_tlc_volume(tmp_path, spark):
    # 세그먼트 A,B는 zone 1(맨해튼), 세그먼트 C는 zone 2(맨해튼).
    # 세그먼트 D는 zone 1이지만 브루클린이라 결과에서 제외되어야 한다.
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B", "C", "D"],
        "zone_id": [1, 1, 2, 1],
        "borough": ["Manhattan", "Manhattan", "Manhattan", "Brooklyn"],
    }).to_parquet(map_zone_segment_path, index=False)

    silver_dir = tmp_path / "silver"
    _write_tlc_silver_fixture(silver_dir, "yellow", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "pickup_location_id": 1,
        "dropoff_location_id": 1,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])

    out_path = build_dim_segment_tlc_volume(
        spark,
        map_zone_segment_path=map_zone_segment_path,
        silver_dir=silver_dir,
        taxi_types=["yellow"],
    )

    validated_path = validate_dim_segment_tlc_volume(out_path, map_zone_segment_path=map_zone_segment_path)
    assert validated_path == out_path

    df = pd.read_parquet(out_path)
    assert len(df) == 3 * 24  # 맨해튼 세그먼트(A,B,C)만 — D는 제외
    assert "D" not in df["segment_id"].values
    hour8 = df[df["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == 1
    assert hour8["B"] == 1
    assert hour8["C"] == 0


def test_validate_dim_segment_tlc_volume_rejects_duplicate_rows(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A"],
        "zone_id": [1],
        "borough": ["Manhattan"],
    }).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "dim_segment_tlc_volume.parquet"
    pd.DataFrame({
        "segment_id": ["A"] * 25,  # 24개여야 하는데 25개 (중복)
        "hour": list(range(24)) + [0],
        "dropoff_count_raw": [0] * 25,
        "tlc_volume": [0.5] * 25,
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError):
        validate_dim_segment_tlc_volume(str(bad_path), map_zone_segment_path=map_zone_segment_path)
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ImportError: cannot import name 'build_dim_segment_tlc_volume'` 로 FAIL

- [ ] **Step 3: 구현 작성**

`src/tlc/gold.py` 상단 import에 추가 (`from src.common.config import SILVER_DIR, TAXI_TYPES` 줄을 아래로 교체):

```python
from src.common.config import BOROUGH_EVENT, SILVER_DIR, TAXI_TYPES
```

파일 끝(`_normalize_tlc_volume` 함수 뒤)에 추가:

```python
def build_dim_segment_tlc_volume(
    spark: SparkSession,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    silver_dir: Path = SILVER_DIR,
    taxi_types: list[str] = TAXI_TYPES,
    borough: str = BOROUGH_EVENT,
) -> str:
    """dim_segment_tlc_volume.parquet을 처음부터 다시 계산해서 저장한다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold 단계에서만 필터링한다.
    """

    zone_hour_counts = _read_zone_hour_counts(spark, silver_dir=silver_dir, taxi_types=taxi_types)

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id", "borough"])
    map_zone_segment = map_zone_segment.loc[map_zone_segment["borough"] == borough, ["segment_id", "zone_id"]]

    expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)
    result = _normalize_tlc_volume(expanded)

    out_path = silver_dir / "dim_segment_tlc_volume.parquet"
    silver_dir.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, index=False)

    logger.info(f"[tlc_gold] dim_segment_tlc_volume 저장 완료: {len(result)}행 -> {out_path}")
    return str(out_path)


def validate_dim_segment_tlc_volume(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    borough: str = BOROUGH_EVENT,
) -> str:
    """dim_segment_tlc_volume.parquet의 최소 불변식을 확인한다."""

    df = pd.read_parquet(path)

    assert not df.duplicated(subset=["segment_id", "hour"]).any(), "(segment_id, hour) 중복 발견"
    assert df["hour"].between(0, 23).all(), "hour가 0~23 범위를 벗어남"
    assert df["tlc_volume"].between(0, 1).all(), "tlc_volume이 0~1 범위를 벗어남"
    assert (df["dropoff_count_raw"] >= 0).all(), "dropoff_count_raw에 음수 있음"

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "borough"])
    segment_count = map_zone_segment.loc[map_zone_segment["borough"] == borough, "segment_id"].nunique()
    expected_rows = segment_count * len(HOURS)
    assert len(df) == expected_rows, f"행 수가 예상과 다릅니다: {len(df)} != {expected_rows}"

    hours_per_segment = df.groupby("segment_id")["hour"].nunique()
    assert (hours_per_segment == len(HOURS)).all(), "일부 세그먼트에 24개 시간대가 다 없음"

    logger.info(f"[tlc_gold] 검증 통과 ({len(df)}행)")
    return path
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 8개 테스트 모두 PASS

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: dim_segment_tlc_volume 빌드/검증 함수 추가"
```

---

### Task 5: 인접 세그먼트 N-hop 탐색

**Files:**
- Modify: `src/tlc/gold.py`
- Modify: `tests/tlc/test_gold.py`

**Interfaces:**
- Produces: `_neighbor_hop_distances(segment_id: str, adjacency: pd.DataFrame, hops: int = DEFAULT_HOPS) -> dict[str, int]`
  - 입력 `adjacency` 컬럼: `segment_id`, `neighbor_segment_id` (양방향으로 이미 저장돼 있다고 가정 — `graph_segment_adjacency.parquet`과 동일한 형식)
  - 출력: `{segment_id: hop_distance}` — 자기 자신은 0, 이후 hop 수만큼만 포함

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_gold.py`에 추가:

```python
from src.tlc.gold import _neighbor_hop_distances


def test_neighbor_hop_distances_walks_graph():
    # 양방향 그래프: A-B, A-C, B-D
    adjacency = pd.DataFrame({
        "segment_id":          ["A", "B", "A", "C", "B", "D"],
        "neighbor_segment_id": ["B", "A", "C", "A", "D", "B"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=3)

    assert result == {"A": 0, "B": 1, "C": 1, "D": 2}


def test_neighbor_hop_distances_respects_hop_limit():
    # A-B-C 체인
    adjacency = pd.DataFrame({
        "segment_id":          ["A", "B", "B", "C"],
        "neighbor_segment_id": ["B", "A", "C", "B"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=1)

    assert result == {"A": 0, "B": 1}  # C는 2단계라 hops=1이면 제외


def test_neighbor_hop_distances_isolated_segment():
    adjacency = pd.DataFrame({
        "segment_id": ["X", "Y"],
        "neighbor_segment_id": ["Y", "X"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=3)  # A는 그래프에 없음

    assert result == {"A": 0}
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ImportError: cannot import name '_neighbor_hop_distances'` 로 FAIL

- [ ] **Step 3: 구현 작성**

`src/tlc/gold.py` 파일 끝에 추가:

```python
def _neighbor_hop_distances(
    segment_id: str,
    adjacency: pd.DataFrame,
    hops: int = DEFAULT_HOPS,
) -> dict[str, int]:
    """segment_id로부터 hops단계 이내(자기 자신 포함)의 세그먼트별 최단 hop 수를 구한다."""

    neighbor_map: dict[str, set[str]] = {}
    for seg, nbr in zip(adjacency["segment_id"], adjacency["neighbor_segment_id"]):
        neighbor_map.setdefault(seg, set()).add(nbr)

    distances: dict[str, int] = {segment_id: 0}
    frontier = {segment_id}

    for depth in range(1, hops + 1):
        next_frontier: set[str] = set()
        for seg in frontier:
            for nbr in neighbor_map.get(seg, set()):
                if nbr not in distances:
                    distances[nbr] = depth
                    next_frontier.add(nbr)
        if not next_frontier:
            break
        frontier = next_frontier

    return distances
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 11개 테스트 모두 PASS

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: 인접 세그먼트 N-hop 탐색 함수 추가"
```

---

### Task 6: 공사 신청용 TLC traffic score 조회 함수

**Files:**
- Modify: `src/tlc/gold.py`
- Modify: `tests/tlc/test_gold.py`

**Interfaces:**
- Consumes: `_neighbor_hop_distances()`(Task 5), `dim_segment_tlc_volume.parquet` 스키마(Task 4)
- Produces: `get_tlc_traffic_score_for_construction(segment_id: str, hour: int, hops: int = DEFAULT_HOPS, gold_path: Path = DIM_SEGMENT_TLC_VOLUME_PATH, adjacency_path: Path = GRAPH_SEGMENT_ADJACENCY_PATH) -> list[dict]`
  - 반환 리스트의 각 원소: `{"segment_id": str, "hop_distance": int, "hour": int, "traffic_score": float}`
  - hop_distance 오름차순 정렬
  - `segment_id`가 gold 테이블에 없으면 `KeyError`
  - `hour`가 0~23 범위 밖이면 `ValueError`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_gold.py`에 추가:

```python
from src.tlc.gold import get_tlc_traffic_score_for_construction


@pytest.fixture
def gold_and_adjacency_paths(tmp_path):
    gold_path = tmp_path / "dim_segment_tlc_volume.parquet"
    pd.DataFrame({
        "segment_id":        ["A", "A", "B", "B", "C", "C"],
        "hour":               [8,   9,   8,   9,   8,   9],
        "dropoff_count_raw": [10,  20,   5,   5,   1,   1],
        "tlc_volume":        [0.9, 0.95, 0.5, 0.5, 0.1, 0.1],
    }).to_parquet(gold_path, index=False)

    adjacency_path = tmp_path / "graph_segment_adjacency.parquet"
    pd.DataFrame({
        "segment_id":          ["A", "B"],
        "neighbor_segment_id": ["B", "A"],
    }).to_parquet(adjacency_path, index=False)

    return gold_path, adjacency_path


def test_get_tlc_traffic_score_for_construction_returns_self_and_neighbors(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    result = get_tlc_traffic_score_for_construction(
        "A", hour=8, hops=3, gold_path=gold_path, adjacency_path=adjacency_path,
    )

    by_segment = {r["segment_id"]: r for r in result}
    assert by_segment["A"] == {"segment_id": "A", "hop_distance": 0, "hour": 8, "traffic_score": 0.9}
    assert by_segment["B"] == {"segment_id": "B", "hop_distance": 1, "hour": 8, "traffic_score": 0.5}
    assert "C" not in by_segment  # A와 인접하지 않음
    assert [r["segment_id"] for r in result] == ["A", "B"]  # hop_distance 오름차순


def test_get_tlc_traffic_score_for_construction_missing_segment_raises(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    with pytest.raises(KeyError):
        get_tlc_traffic_score_for_construction(
            "Z", hour=8, gold_path=gold_path, adjacency_path=adjacency_path,
        )


def test_get_tlc_traffic_score_for_construction_invalid_hour_raises(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    with pytest.raises(ValueError):
        get_tlc_traffic_score_for_construction(
            "A", hour=24, gold_path=gold_path, adjacency_path=adjacency_path,
        )
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: `ImportError: cannot import name 'get_tlc_traffic_score_for_construction'` 로 FAIL

- [ ] **Step 3: 구현 작성**

`src/tlc/gold.py` 파일 끝에 추가:

```python
def get_tlc_traffic_score_for_construction(
    segment_id: str,
    hour: int,
    hops: int = DEFAULT_HOPS,
    gold_path: Path = DIM_SEGMENT_TLC_VOLUME_PATH,
    adjacency_path: Path = GRAPH_SEGMENT_ADJACENCY_PATH,
) -> list[dict]:
    """공사 위치 segment_id + 인접 hops단계 이내 세그먼트들의 TLC 기반 점수.

    지금은 tlc_volume 하나만 반영한 임시 점수다. 나중에 팀 공용
    scoring/traffic_score.py가 다른 요인(중심성, capacity, event, closure)과
    합칠 때 이 값을 가져다 쓸 수 있다.
    """

    if not 0 <= hour <= 23:
        raise ValueError(f"hour는 0~23 범위여야 합니다: {hour}")

    gold = pd.read_parquet(gold_path)
    if segment_id not in gold["segment_id"].values:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")

    adjacency = pd.read_parquet(adjacency_path, columns=["segment_id", "neighbor_segment_id"])
    hop_distances = _neighbor_hop_distances(segment_id, adjacency, hops=hops)

    hour_scores = gold[gold["hour"] == hour].set_index("segment_id")["tlc_volume"]

    results = [
        {
            "segment_id": seg,
            "hop_distance": dist,
            "hour": hour,
            "traffic_score": float(hour_scores.loc[seg]),
        }
        for seg, dist in hop_distances.items()
        if seg in hour_scores.index
    ]

    return sorted(results, key=lambda r: r["hop_distance"])
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 14개 테스트 모두 PASS

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/gold.py tests/tlc/test_gold.py
git commit -m "feat: 공사 신청용 TLC traffic score 조회 함수 추가"
```

---

### Task 7: 직접 실행 진입점 추가

**Files:**
- Modify: `src/tlc/gold.py`

**Interfaces:**
- Consumes: `build_dim_segment_tlc_volume()`, `validate_dim_segment_tlc_volume()` (둘 다 Task 4)
- Produces: 없음 (스크립트 진입점)

- [ ] **Step 1: `__main__` 블록 추가**

`src/tlc/gold.py` 맨 끝에 추가 (`get_tlc_traffic_score_for_construction` 함수 뒤):

```python
if __name__ == "__main__":
    from src.common.spark import get_spark

    spark_session = get_spark()
    try:
        out = build_dim_segment_tlc_volume(spark_session)
        validate_dim_segment_tlc_volume(out)
    finally:
        spark_session.stop()
```

이 진입점은 `lion/traffic_score.py`와 마찬가지로 Airflow DAG에 연결하지 않은
스크립트 형태다. 실제로 돌리려면 `docker-compose up`으로 spark-master/worker가
떠 있어야 하므로, 이번 계획에서는 자동 테스트하지 않는다 (Task 1~6의 유닛
테스트가 로직을 이미 다 검증했다).

- [ ] **Step 2: 문법 오류 없는지 확인**

```bash
python -m py_compile src/tlc/gold.py
```

Expected: 아무 출력 없이 종료 (에러 없음)

- [ ] **Step 3: 전체 테스트 스위트 마지막으로 한 번 더 실행**

```bash
python -m pytest tests/tlc/test_gold.py -v
```

Expected: 14개 테스트 모두 PASS

- [ ] **Step 4: 커밋**

```bash
git add src/tlc/gold.py
git commit -m "feat: dim_segment_tlc_volume 직접 실행 진입점 추가"
```
