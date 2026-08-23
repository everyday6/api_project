# Type1 collected_date Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SegmentMetricsType1`의 버킷 항목(sk="HHMM")에 원본 데이터의 관측 날짜(`collected_date`)를 저장해서, 그 값이 며칠자 데이터로 계산됐는지 DynamoDB에서 바로 확인할 수 있게 한다.

**Architecture:** `src/nav_time/gold2.py`의 `compute_time_seconds`가 이미 쓰고 있는 `observed_at` 컬럼에서 버킷별 최신 날짜를 뽑아 `collected_date` 컬럼으로 집계에 추가하고, `to_dynamodb_items`가 그 컬럼을 ISO 날짜 문자열로 변환해 버킷 항목에만 실어 보낸다. AVG 항목, DynamoDB 스키마, API는 건드리지 않는다.

**Tech Stack:** PySpark(`pyspark.sql.functions.to_date`/`max`), 기존 pytest + moto(`mock_aws`) 테스트 스택 그대로 사용.

## Global Constraints

- `src/nav_time/gold2.py` 한 파일만 프로덕션 코드를 수정한다 — `src/common/dynamodb.py`, `src/common/config.py`, API/서빙 레이어(`src/serving/*`)는 변경 금지 (스펙 5절)
- `collected_date`는 버킷 항목에만 추가하고 AVG/GLOBAL 항목엔 추가하지 않는다 (스펙 2절)
- DynamoDB 테이블 스키마·키 구조·기존 데이터 백필은 변경/수행하지 않는다 (스펙 2절)
- 값 형식은 ISO 날짜 문자열(`"YYYY-MM-DD"`)이다 (스펙 4절)

---

## File Structure

- **Modify: `src/nav_time/gold2.py`** — `compute_time_seconds`(버킷별 `collected_date` 집계 추가), `to_dynamodb_items`(버킷 항목에 `collected_date` 필드 추가)
- **Modify: `tests/nav_time/test_gold2.py`** — 새 `collected_date` 관련 테스트 추가, 기존 `to_dynamodb_items` 테스트들의 입력 DataFrame에 `collected_date` 컬럼 추가(Task 2에서 프로덕션 코드가 이 필드를 필수로 읽게 되므로)

---

### Task 1: compute_time_seconds에 collected_date 컬럼 추가

**Files:**
- Modify: `src/nav_time/gold2.py:17-90` (import 및 `compute_time_seconds`)
- Test: `tests/nav_time/test_gold2.py`

**Interfaces:**
- Consumes: 기존 `compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame`의 입력 그대로 (변경 없음)
- Produces: 반환 DataFrame에 새 컬럼 `collected_date`(`DateType`) 추가 — Task 2가 `row["collected_date"]`로 읽는다

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/nav_time/test_gold2.py` 상단 import를 아래로 교체한다 (`date` 추가):

```python
from datetime import date, datetime
```

같은 파일의 `test_compute_time_seconds_excludes_zero_speed_segment` 함수 바로 아래에 테스트 2개를 추가한다:

```python
def test_compute_time_seconds_includes_collected_date(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["collected_date"] == date(2026, 8, 21)


def test_compute_time_seconds_collected_date_uses_latest_observed_at_when_dates_mixed(spark):
    # 같은 세그먼트/버킷(0000)에 서로 다른 날짜의 판독값이 섞여 들어오는 경우
    # (자정 경계 등) -> 가장 최근 observed_at의 날짜를 collected_date로 쓴다.
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 20.0, "observed_at": datetime(2026, 8, 21, 0, 5)},
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 22, 0, 10)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "0000"
    assert result[0]["collected_date"] == date(2026, 8, 22)
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_time/test_gold2.py -k collected_date -v`
Expected: 두 테스트 모두 FAIL — `result[0]["collected_date"]`가 `ValueError`를 던짐(pyspark `Row`는 없는 필드 이름으로 접근하면 `ValueError`를 던진다 — "collected_date" 컬럼이 아직 select에 없기 때문)

- [ ] **Step 3: 최소 구현**

`src/nav_time/gold2.py`의 import 블록을 아래로 교체한다:

```python
from pyspark.sql.functions import (
    col,
    concat,
    count as spark_count,
    floor,
    hour,
    lpad,
    max as spark_max,
    minute,
    row_number,
    sum as spark_sum,
    to_date,
)
```

`compute_time_seconds` 함수 본문을 아래로 교체한다:

```python
def compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame:
    """(segment_id, speed, observed_at)를 30분 버킷별 가중평균 통행시간(초)으로 집계한다.

    한 버킷 안에서 시간순으로 매긴 순위(rank)를 가중치로 쓴다 — n개 판독값이면
    1:2:...:n 비율(최근 값일수록 크게), 삼각수 n*(n+1)/2로 정규화한다.

    collected_date는 그 버킷을 구성한 판독값들의 observed_at 중 가장 최근 값의
    날짜다 — DynamoDB에 저장된 버킷 값이 며칠자 원본 데이터로 계산됐는지 표시하기
    위함(docs/superpowers/specs/2026-08-24-type1-collected-date-design.md).
    """

    spark = silver2_df.sparkSession
    length_df = spark.createDataFrame(dim_segment_length_df[["segment_id", "length_ft"]])

    bucketed = silver2_df.withColumn("bucket", _bucket_column())

    window_spec = Window.partitionBy("segment_id", "bucket").orderBy("observed_at")
    ranked = bucketed.withColumn("rank", row_number().over(window_spec))

    counts = ranked.groupBy("segment_id", "bucket").agg(spark_count("*").alias("n"))
    ranked = ranked.join(counts, on=["segment_id", "bucket"])

    weighted = ranked.withColumn(
        "weighted_speed",
        col("speed") * col("rank") / (col("n") * (col("n") + 1) / 2),
    )

    bucket_avg_speed = (
        weighted.groupBy("segment_id", "bucket")
        .agg(
            spark_sum("weighted_speed").alias("avg_speed"),
            to_date(spark_max("observed_at")).alias("collected_date"),
        )
        .filter(col("avg_speed") > 0)
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        "collected_date",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_time/test_gold2.py -v`
Expected: 전체 PASS (기존 `test_compute_time_seconds_*` 4개 + 새 테스트 2개 포함, `to_dynamodb_items` 관련 테스트는 아직 Task 2 전이라 그대로 통과)

- [ ] **Step 5: 커밋**

```bash
git add src/nav_time/gold2.py tests/nav_time/test_gold2.py
git commit -m "feat: compute_time_seconds에 collected_date 컬럼 추가"
```

---

### Task 2: to_dynamodb_items 버킷 항목에 collected_date 추가

**Files:**
- Modify: `src/nav_time/gold2.py:93-116` (`to_dynamodb_items`)
- Test: `tests/nav_time/test_gold2.py:101-226`

**Interfaces:**
- Consumes: Task 1이 만든 `compute_time_seconds`의 출력 컬럼 `collected_date`(`DateType`, `row["collected_date"]`로 접근 가능)
- Produces: `to_dynamodb_items`가 반환하는 버킷 항목 dict에 `"collected_date": "<YYYY-MM-DD>"`(문자열) 키 추가. AVG 항목엔 이 키가 없다.

- [ ] **Step 1: 기존 테스트 업데이트 + 실패하는 새 테스트 작성**

`tests/nav_time/test_gold2.py`에서 `test_to_dynamodb_items_incrementally_updates_avg`부터
`test_to_dynamodb_items_folds_multiple_buckets_of_same_segment_sequentially`까지(101번째 줄부터
215번째 줄까지, `test_write_to_dynamodb_calls_batch_write_and_returns_count` 바로 앞까지)를
통째로 아래 코드로 교체한다 — 각 `spark.createDataFrame` 입력에 `"collected_date": date(2026, 8, 21)`을
추가하고, 새 테스트 2개(`test_to_dynamodb_items_includes_collected_date_in_bucket_items`,
`test_to_dynamodb_items_avg_item_has_no_collected_date`)를 끝에 더한다:

```python
@mock_aws
def test_to_dynamodb_items_incrementally_updates_avg(spark):
    _create_test_table()

    # 1) 빈 테이블 -> 세그먼트 1의 버킷 1200에 30 upsert -> AVG=30, count=1
    df1 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
    ])
    items1 = gold2.to_dynamodb_items(df1, TABLE_NAME)
    gold2.write_to_dynamodb(items1, TABLE_NAME)

    by_sk1 = {(i["segment_id"], i["sk"]): i for i in items1}
    assert by_sk1[("1", "1200")]["value"] == 30
    assert by_sk1[("1", "AVG")]["value"] == 30
    assert by_sk1[("1", "AVG")]["count"] == 1

    # 2) 새 버킷 1230에 50 upsert -> AVG=(30+50)/2=40, count=2
    df2 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": date(2026, 8, 21)},
    ])
    items2 = gold2.to_dynamodb_items(df2, TABLE_NAME)
    gold2.write_to_dynamodb(items2, TABLE_NAME)

    by_sk2 = {(i["segment_id"], i["sk"]): i for i in items2}
    assert by_sk2[("1", "1230")]["value"] == 50
    assert by_sk2[("1", "AVG")]["value"] == 40
    assert by_sk2[("1", "AVG")]["count"] == 2

    # 3) 기존 버킷 1200을 60으로 교체 -> AVG=(60+50)/2=55, count는 그대로 2
    df3 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 60.0, "collected_date": date(2026, 8, 21)},
    ])
    items3 = gold2.to_dynamodb_items(df3, TABLE_NAME)
    gold2.write_to_dynamodb(items3, TABLE_NAME)

    by_sk3 = {(i["segment_id"], i["sk"]): i for i in items3}
    assert by_sk3[("1", "1200")]["value"] == 60
    assert by_sk3[("1", "AVG")]["value"] == 55
    assert by_sk3[("1", "AVG")]["count"] == 2


@mock_aws
def test_to_dynamodb_items_handles_legacy_avg_item_without_count(spark):
    # 레거시 AVG 레코드: count 필드 없이 저장된 옛 버전 데이터를 시뮬레이션.
    client = _create_test_table()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "AVG"},
            "value": {"N": "42"},
        },
    )

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
    ])

    # KeyError 없이 정상 동작해야 한다.
    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    # count 없던 레거시 레코드는 old_count=0으로 취급 -> new_count=1
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == round(42.0 + (30.0 - 42.0) / 1)


@mock_aws
def test_to_dynamodb_items_resets_legacy_avg_when_bucket_already_exists(spark):
    # 레거시 AVG(count 없음) + 이미 존재하는 버킷 값이 같이 있는 상태.
    # old_count=0을 "1개짜리 평균"으로 착각해 델타를 통째로 반영하면 평균이
    # 무한정 발산한다(회귀 재현 시 42 -> -130 -> -275 -> ... 로 계속 떨어짐).
    # count를 모르면 old_avg를 버리고 리셋해야 한다.
    client = _create_test_table()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "AVG"},
            "value": {"N": "42"},
        },
    )
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "1200"},
            "value": {"N": "100"},
        },
    )

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
    ])
    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == 30


@mock_aws
def test_to_dynamodb_items_folds_multiple_buckets_of_same_segment_sequentially(spark):
    # 한 번의 호출에 같은 세그먼트의 버킷이 2개(수집 구간 경계 겹침 등으로) 동시에
    # 들어와도, 순차적으로 접어(fold) 반영해서 AVG가 정확히 계산되고 세그먼트당
    # AVG 항목이 딱 1개만 나와야 한다.
    _create_test_table()

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": date(2026, 8, 21)},
    ])

    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    avg_items = [i for i in items if i["segment_id"] == "1" and i["sk"] == "AVG"]
    assert len(avg_items) == 1
    assert avg_items[0]["value"] == 40  # (30+50)/2
    assert avg_items[0]["count"] == 2

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "1230")]["value"] == 50


@mock_aws
def test_to_dynamodb_items_includes_collected_date_in_bucket_items(spark):
    _create_test_table()

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
    ])

    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["collected_date"] == "2026-08-21"


@mock_aws
def test_to_dynamodb_items_avg_item_has_no_collected_date(spark):
    _create_test_table()

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21)},
    ])

    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert "collected_date" not in by_sk[("1", "AVG")]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/nav_time/test_gold2.py -k "includes_collected_date_in_bucket_items or avg_item_has_no_collected_date" -v`
Expected: `test_to_dynamodb_items_includes_collected_date_in_bucket_items`가 FAIL (`KeyError: 'collected_date'`), `test_to_dynamodb_items_avg_item_has_no_collected_date`는 현재 AVG에 `collected_date` 자체가 없는 게 이미 사실이라 이 시점엔 PASS할 수 있음 — Step 1에서 다른 기존 테스트가 깨지지 않는지도 함께 확인:

Run: `pytest tests/nav_time/test_gold2.py -v`
Expected: `test_to_dynamodb_items_includes_collected_date_in_bucket_items` 하나만 FAIL, 나머지는 전부 PASS

- [ ] **Step 3: 최소 구현**

`src/nav_time/gold2.py`의 `to_dynamodb_items` 함수에서 `bucket_items` 생성 부분을 아래로 교체한다:

```python
    bucket_items = [
        {
            "segment_id": row["segment_id"],
            "sk": row["bucket"],
            "value": round(row["time_seconds"]),
            "collected_date": row["collected_date"].isoformat(),
        }
        for row in rows
    ]
```

(이 리스트 컴프리헨션 윗줄의 `rows = bucket_df.collect()`와 아랫줄의 `if not bucket_items: return []`
이하 로직은 그대로 둔다.)

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/nav_time/test_gold2.py -v`
Expected: 전체 PASS

- [ ] **Step 5: 전체 테스트 스위트 확인 및 커밋**

Run: `pytest tests/nav_time/ -v`
Expected: 전체 PASS

```bash
git add src/nav_time/gold2.py tests/nav_time/test_gold2.py
git commit -m "feat: to_dynamodb_items 버킷 항목에 collected_date 추가"
```
