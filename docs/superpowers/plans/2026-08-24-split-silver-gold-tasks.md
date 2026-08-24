# segment_time_pipeline Silver/Gold Task 분리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `spark_jobs/nav_time_job.py` 하나(Bronze→Silver1→Silver2→Gold1→Gold2를 EMR job 하나 안에서 전부 처리)를 `nav_time_silver_job.py`(Bronze→Silver1→Silver2)와 `nav_time_gold_job.py`(Silver2→Gold1→Gold2)로 쪼개고, `segment_time_pipeline` DAG도 `submit_nav_time_job` task 하나를 `submit_silver_job`→`submit_gold_job` 두 task로 나눠서, EMR job이 실패했을 때 Airflow 화면에서 바로 어느 단계인지 알 수 있게 한다.

**Architecture:** 기존 함수(`clean_speed_silver1`, `build_segment_speed_silver2`, `filter_valid_speed`, `compute_time_seconds`, `to_serving_items`, `write_to_rds`)는 전혀 안 바꾼다 — 두 엔트리포인트 스크립트에 어떻게 나눠 호출하는지만 바뀐다. Silver job의 결과(Silver2 DataFrame)는 `EMR_JOBS_DIR/outputs/`에 parquet로 저장되고, Gold job이 그 경로를 읽어서 이어간다.

**Tech Stack:** 기존 `src/common/emr_serverless.py`(`run_spark_job`, `read_json_result`), `cloudpathlib.S3Path`. 새 인프라 없음.

## Global Constraints

- Bronze 쪽(`collect_bronze`, `validate_bronze`)은 이번 작업 범위 밖 — 손대지 않는다.
- Silver/Gold 각 단계의 실제 계산 로직은 바꾸지 않는다 — 기존 함수를 그대로 재사용하고, 어느 스크립트에서 어떤 순서로 호출하는지만 바뀐다.
- Silver2 중간 산출물은 도메인 데이터 폴더(`SILVER2_DIR`)가 아니라 `EMR_JOBS_DIR/outputs/`에 run_id로 구분해서 저장한다.
- 실패 알림은 새로 만들지 않는다 — 두 task 모두 일반 `@task`라서 `run_spark_job`이 예외를 던지면 Airflow task가 실패 처리되고, DAG의 기존 `on_failure_callback=notify_slack_failure`가 자동으로 처리한다.
- Gold job도 `dim_segment_path`를 자기 인자로 받는다 — `compute_time_seconds`가 `length_ft` 조회에 필요하기 때문.
- 새 엔트리포인트 스크립트의 테스트는 이미 별도로 테스트된 하위 함수(`clean_speed_silver1` 등)를 mock으로 대체해서 "이 스크립트가 올바른 순서/인자로 그 함수들을 호출하고, 결과를 올바르게 저장/전달하는지"(=새로 생긴 배선 로직)만 검증한다 — 하위 함수 자체의 정확성은 기존 테스트(`tests/speed/test_silver1.py`, `tests/silver2/test_segment_speed.py`, `tests/nav_time/test_gold1.py`, `tests/nav_time/test_gold2.py`)가 이미 커버한다.

---

### Task 1: `nav_time_silver_job.py` 엔트리포인트

**Files:**
- Create: `spark_jobs/nav_time_silver_job.py`
- Test: `tests/spark_jobs/__init__.py` (새 디렉터리라 필요), `tests/spark_jobs/test_nav_time_silver_job.py`

**Interfaces:**
- Consumes: `src.speed.silver1.clean_speed_silver1(df) -> DataFrame`, `src.silver2.segment_speed.build_segment_speed_silver2(speed_silver1_df, dim_segment_df) -> DataFrame` (기존 함수, 시그니처 변경 없음)
- Produces: `run(speed_bronze_path: str, dim_segment_path: str, silver2_output: str, output_s3: str) -> None` — Silver2 결과를 `silver2_output`에 parquet로 저장하고, `output_s3`에 `{"row_count": N}` JSON을 저장한다. Task 3이 이 스크립트를 EMR entry point로 제출한다.

- [ ] **Step 1: 테스트 디렉터리 초기화**

```bash
mkdir -p tests/spark_jobs
touch tests/spark_jobs/__init__.py
```

- [ ] **Step 2: 실패하는 테스트 작성**

`tests/spark_jobs/test_nav_time_silver_job.py`:

```python
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from spark_jobs import nav_time_silver_job


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nav_time_silver_job_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_calls_silver_functions_in_order_and_writes_output(tmp_path, spark):
    bronze_path = tmp_path / "bronze.parquet"
    pd.DataFrame([{"link_id": "1", "speed": "29.82", "data_as_of": "2026-08-24T12:00:00.000"}]).to_parquet(
        bronze_path, index=False
    )

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame([{"segment_id": "seg-1", "length_ft": 500.0}]).to_parquet(dim_segment_path, index=False)

    silver2_output = str(tmp_path / "silver2.parquet")
    output_s3 = tmp_path / "result.json"

    fake_silver1_df = spark.createDataFrame([{"link_id": "1"}])
    fake_silver2_df = spark.createDataFrame(
        [{"segment_id": "seg-1", "speed": 29.82, "observed_at": "2026-08-24T12:00:00"}]
    )

    with patch.object(nav_time_silver_job, "S3Path", Path), \
         patch.object(
             nav_time_silver_job, "clean_speed_silver1", return_value=fake_silver1_df
         ) as mock_silver1, \
         patch.object(
             nav_time_silver_job, "build_segment_speed_silver2", return_value=fake_silver2_df
         ) as mock_silver2:
        nav_time_silver_job.run(
            speed_bronze_path=str(bronze_path),
            dim_segment_path=str(dim_segment_path),
            silver2_output=silver2_output,
            output_s3=str(output_s3),
        )

    # clean_speed_silver1은 Bronze DataFrame 하나만 받는다.
    mock_silver1.assert_called_once()
    bronze_df_arg = mock_silver1.call_args.args[0]
    assert bronze_df_arg.collect()[0]["link_id"] == "1"

    # build_segment_speed_silver2는 clean_speed_silver1의 결과 + dim_segment_df를 받는다.
    mock_silver2.assert_called_once()
    silver1_arg, dim_segment_arg = mock_silver2.call_args.args
    assert silver1_arg is fake_silver1_df
    assert dim_segment_arg["segment_id"].tolist() == ["seg-1"]

    saved = spark.read.parquet(silver2_output)
    assert saved.count() == 1
    assert saved.collect()[0]["segment_id"] == "seg-1"

    result = json.loads(Path(output_s3).read_text())
    assert result == {"row_count": 1}


def test_main_requires_all_four_arguments():
    with patch("sys.argv", ["nav_time_silver_job.py"]):
        with pytest.raises(SystemExit):
            nav_time_silver_job.main()
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `pytest tests/spark_jobs/test_nav_time_silver_job.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'spark_jobs.nav_time_silver_job'`

- [ ] **Step 4: 최소 구현 작성**

`spark_jobs/nav_time_silver_job.py`:

```python
"""
EMR Serverless 잡 엔트리포인트 — 속도 Bronze -> Silver2(LION 세그먼트 단위)

Bronze -> Silver1(정제) -> Silver2(LION 세그먼트 매핑)까지만 처리하고
결과를 S3에 parquet로 남긴다. 이어지는 Gold1/Gold2는
nav_time_gold_job.py가 이 결과를 읽어서 별도 EMR job으로 처리한다 -
하나로 묶여있던 job을 Silver/Gold 두 Airflow task로 나눠서, 실패했을 때
Airflow 화면에서 바로 어느 단계인지 알 수 있게 하기 위함
(docs/superpowers/specs/2026-08-24-split-silver-gold-tasks-design.md 참고).

인자:
  --speed-bronze-path : 속도 Bronze parquet 경로(Airflow가 수집한 원본, 정제 전)
  --dim-segment-path   : LION Gold2 dim_segment.parquet 경로
                         (segment_id, geometry, is_routable, length_ft)
  --silver2-output      : Silver2 결과를 저장할 parquet 경로(다음 EMR job이 읽음)
  --output-s3           : 처리 결과({"row_count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.silver2.segment_speed import build_segment_speed_silver2
from src.speed.silver1 import clean_speed_silver1


def run(speed_bronze_path: str, dim_segment_path: str, silver2_output: str, output_s3: str) -> None:
    spark = SparkSession.builder.appName("nav-time-silver").getOrCreate()

    try:
        bronze_df = spark.read.parquet(speed_bronze_path)
        dim_segment_df = pd.read_parquet(dim_segment_path)

        speed_silver1_df = clean_speed_silver1(bronze_df)
        silver2_df = build_segment_speed_silver2(speed_silver1_df, dim_segment_df)

        row_count = silver2_df.count()
        silver2_df.write.parquet(silver2_output, mode="overwrite")
    finally:
        spark.stop()

    S3Path(output_s3).write_text(json.dumps({"row_count": row_count}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed-bronze-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--silver2-output", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    run(
        speed_bronze_path=args.speed_bronze_path,
        dim_segment_path=args.dim_segment_path,
        silver2_output=args.silver2_output,
        output_s3=args.output_s3,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/spark_jobs/test_nav_time_silver_job.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: 커밋**

```bash
git add spark_jobs/nav_time_silver_job.py tests/spark_jobs/__init__.py tests/spark_jobs/test_nav_time_silver_job.py
git commit -m "feat: nav_time_silver_job EMR 엔트리포인트 추가 (Bronze->Silver2)"
```

---

### Task 2: `nav_time_gold_job.py` 엔트리포인트

**Files:**
- Create: `spark_jobs/nav_time_gold_job.py`
- Test: `tests/spark_jobs/test_nav_time_gold_job.py`

**Interfaces:**
- Consumes: `src.nav_time.gold1.filter_valid_speed(df) -> DataFrame`, `src.nav_time.gold2.compute_time_seconds(gold1_df, dim_segment_length_df) -> DataFrame`, `src.nav_time.gold2.to_serving_items(bucket_df, table_name) -> list[dict]`, `src.nav_time.gold2.write_to_rds(items, table_name) -> int` (전부 기존 함수, 시그니처 변경 없음)
- Produces: `run(silver2_path: str, dim_segment_path: str, serving_table: str, output_s3: str) -> None` — `output_s3`에 `{"count": N}` JSON을 저장한다. Task 3이 이 스크립트를 EMR entry point로 제출한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/spark_jobs/test_nav_time_gold_job.py`:

```python
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from spark_jobs import nav_time_gold_job


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nav_time_gold_job_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_calls_gold_functions_in_order_and_writes_output(tmp_path, spark):
    silver2_path = tmp_path / "silver2.parquet"
    spark.createDataFrame(
        [{"segment_id": "seg-1", "speed": 29.82, "observed_at": "2026-08-24T12:00:00"}]
    ).write.parquet(str(silver2_path))

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame([{"segment_id": "seg-1", "length_ft": 500.0}]).to_parquet(dim_segment_path, index=False)

    output_s3 = tmp_path / "result.json"
    serving_table = "test_segment_metrics_type1"

    fake_gold1_df = spark.createDataFrame([{"segment_id": "seg-1", "speed": 29.82}])
    fake_bucket_df = spark.createDataFrame([{"segment_id": "seg-1", "bucket": "1200", "time_seconds": 61.0}])
    fake_items = [{"segment_id": "seg-1", "time": "1200", "value": 61}]

    with patch.object(nav_time_gold_job, "S3Path", Path), \
         patch.object(nav_time_gold_job, "filter_valid_speed", return_value=fake_gold1_df) as mock_gold1, \
         patch.object(
             nav_time_gold_job, "compute_time_seconds", return_value=fake_bucket_df
         ) as mock_gold2, \
         patch.object(
             nav_time_gold_job, "to_serving_items", return_value=fake_items
         ) as mock_to_items, \
         patch.object(nav_time_gold_job, "write_to_rds", return_value=1) as mock_write:
        nav_time_gold_job.run(
            silver2_path=str(silver2_path),
            dim_segment_path=str(dim_segment_path),
            serving_table=serving_table,
            output_s3=str(output_s3),
        )

    # filter_valid_speed는 Silver2 DataFrame 하나만 받는다.
    mock_gold1.assert_called_once()
    silver2_df_arg = mock_gold1.call_args.args[0]
    assert silver2_df_arg.collect()[0]["segment_id"] == "seg-1"

    # compute_time_seconds는 filter_valid_speed 결과 + (segment_id, length_ft) 컬럼만 받는다.
    mock_gold2.assert_called_once()
    gold1_arg, length_df_arg = mock_gold2.call_args.args
    assert gold1_arg is fake_gold1_df
    assert length_df_arg.columns.tolist() == ["segment_id", "length_ft"]

    # to_serving_items는 compute_time_seconds 결과 + 테이블명을 받는다.
    mock_to_items.assert_called_once_with(fake_bucket_df, serving_table)

    # write_to_rds는 to_serving_items 결과 + 테이블명을 받는다.
    mock_write.assert_called_once_with(fake_items, serving_table)

    result = json.loads(Path(output_s3).read_text())
    assert result == {"count": 1}


def test_main_requires_all_four_arguments():
    with patch("sys.argv", ["nav_time_gold_job.py"]):
        with pytest.raises(SystemExit):
            nav_time_gold_job.main()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/spark_jobs/test_nav_time_gold_job.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'spark_jobs.nav_time_gold_job'`

- [ ] **Step 3: 최소 구현 작성**

`spark_jobs/nav_time_gold_job.py`:

```python
"""
EMR Serverless 잡 엔트리포인트 — Silver2(LION 세그먼트 단위) -> type1(시간) RDS upsert

nav_time_silver_job.py가 만들어둔 Silver2 parquet을 읽어서 Gold1(필터)
-> Gold2(버킷 평균+시간 계산+RDS upsert)를 처리한다
(docs/superpowers/specs/2026-08-24-split-silver-gold-tasks-design.md 참고).

인자:
  --silver2-path  : nav_time_silver_job.py가 저장한 Silver2 parquet 경로
  --dim-segment-path : LION Gold2 dim_segment.parquet 경로
                       (segment_id, geometry, is_routable, length_ft)
  --serving-table  : upsert할 RDS 테이블명
  --output-s3      : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_valid_speed
from src.nav_time.gold2 import compute_time_seconds, to_serving_items, write_to_rds


def run(silver2_path: str, dim_segment_path: str, serving_table: str, output_s3: str) -> None:
    spark = SparkSession.builder.appName("nav-time-gold").getOrCreate()

    try:
        silver2_df = spark.read.parquet(silver2_path)
        dim_segment_df = pd.read_parquet(dim_segment_path)

        gold1_df = filter_valid_speed(silver2_df)

        bucket_df = compute_time_seconds(gold1_df, dim_segment_df[["segment_id", "length_ft"]])
        items = to_serving_items(bucket_df, serving_table)
        count = write_to_rds(items, serving_table)
    finally:
        spark.stop()

    S3Path(output_s3).write_text(json.dumps({"count": count}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver2-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--serving-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    run(
        silver2_path=args.silver2_path,
        dim_segment_path=args.dim_segment_path,
        serving_table=args.serving_table,
        output_s3=args.output_s3,
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/spark_jobs/test_nav_time_gold_job.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: 커밋**

```bash
git add spark_jobs/nav_time_gold_job.py tests/spark_jobs/test_nav_time_gold_job.py
git commit -m "feat: nav_time_gold_job EMR 엔트리포인트 추가 (Silver2->RDS)"
```

---

### Task 3: DAG 연결 및 옛 엔트리포인트 삭제

**Files:**
- Modify: `dags/segment_time_pipeline.py`
- Delete: `spark_jobs/nav_time_job.py`

**Interfaces:**
- Consumes: Task 1의 `spark_jobs/nav_time_silver_job.py`, Task 2의 `spark_jobs/nav_time_gold_job.py` (경로만 참조, import는 안 함 — EMR entry point script라 `run_spark_job`에 파일 경로로 전달됨)

- [ ] **Step 1: `dags/segment_time_pipeline.py`의 `submit_nav_time_job` task를 아래 두 task로 교체**

기존:

```python
    @task
    def submit_nav_time_job(speed_bronze_path: str) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_job.py",
            entry_point_args=[
                "--speed-bronze-path", speed_bronze_path,
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--serving-table", SERVING_TABLE_TYPE1,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))
```

다음으로 교체:

```python
    @task
    def submit_silver_job(speed_bronze_path: str) -> dict:
        run_id = uuid.uuid4().hex
        silver2_path = EMR_JOBS_DIR / "outputs" / f"nav_time_silver2_{run_id}.parquet"
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_silver_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-silver-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_silver_job.py",
            entry_point_args=[
                "--speed-bronze-path", speed_bronze_path,
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--silver2-output", str(silver2_path),
                "--output-s3", str(output_s3),
            ],
        )

        result = read_json_result(str(output_s3))
        return {"silver2_path": str(silver2_path), **result}

    @task
    def submit_gold_job(silver_result: dict) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_gold_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-gold-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_gold_job.py",
            entry_point_args=[
                "--silver2-path", silver_result["silver2_path"],
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--serving-table", SERVING_TABLE_TYPE1,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))
```

- [ ] **Step 2: task 그래프 배선 수정**

기존:

```python
    new_data = check_new_data()
    bronze_path = collect_bronze()
    bronze_path.set_upstream(new_data)

    bronze_valid = validate_bronze(bronze_path)

    dim_segment_ready = check_dim_segment_exists()

    submit_result = submit_nav_time_job(bronze_path)
    submit_result.set_upstream(dim_segment_ready)
    submit_result.set_upstream(bronze_valid)
```

다음으로 교체:

```python
    new_data = check_new_data()
    bronze_path = collect_bronze()
    bronze_path.set_upstream(new_data)

    bronze_valid = validate_bronze(bronze_path)

    dim_segment_ready = check_dim_segment_exists()

    silver_result = submit_silver_job(bronze_path)
    silver_result.set_upstream(dim_segment_ready)
    silver_result.set_upstream(bronze_valid)

    gold_result = submit_gold_job(silver_result)
```

- [ ] **Step 3: 옛 엔트리포인트 삭제**

```bash
rm spark_jobs/nav_time_job.py
```

- [ ] **Step 4: DAG가 정상적으로 import되는지 확인**

Run: `python3 -c "from dags.segment_time_pipeline import segment_time_pipeline"`
Expected: 에러 없이 조용히 끝남(예외 없음)

- [ ] **Step 5: 전체 관련 테스트 확인**

Run: `pytest tests/spark_jobs/ tests/speed/ tests/nav_time/ tests/silver2/ -v`
Expected: PASS (Task 1/2에서 추가한 것 포함 전부 — `nav_time_job.py`를 참조하는 테스트가 없었으므로 삭제로 인한 실패 없음)

- [ ] **Step 6: 커밋**

```bash
git add dags/segment_time_pipeline.py
git rm spark_jobs/nav_time_job.py
git commit -m "feat: segment_time_pipeline의 EMR job을 Silver/Gold 두 task로 분리"
```
