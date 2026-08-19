# TLC Bronze Great Expectations 데이터 품질 검증 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** TLC Bronze 파일이 Silver로 넘어가기 전에 Great Expectations로 스키마/값 수준 검증을 실행하고, 심각도에 따라 파일을 제외(critical)하거나 로그만 남기는(log-only) 자동 대응을 추가한다.

**Architecture:** `src/common/gx.py`는 "Spark DataFrame + Expectation 목록을 받아 검증 실행" 만 책임지는 공통 러너다. `src/tlc/expectations.py`는 taxi_type(yellow/green/fhv/fhvhv)별 Expectation 정의를 제공한다. `src/tlc/bronze_validation.py`는 이 둘을 조합해 파일 단위로 판정하고, taxi_type 청크 단위로 Spark 세션을 재사용하면서도 파일별로 개별 판정해 한 파일의 실패가 같은 청크의 다른 파일을 막지 않게 한다. `dags/tlc_pipeline.py`는 `store_bronze`와 `build_silver` 사이에 이 검증 Task를 끼워 넣는다.

**Tech Stack:** Python 3.11, PySpark 4.0.4, Great Expectations 1.20+ (Fluent/GX 1.x API), Apache Airflow(TaskFlow API), pytest.

## Global Constraints

- Python 3.11 (Docker 이미지: `apache/airflow:3.3.0-python3.11`) — 변경 없음.
- `pyspark==4.0.4` — 기존 고정 버전, 변경 없음.
- `great_expectations>=1.20.0` — 스파이크 테스트로 PySpark 4.0.4와의 호환성을 직접 검증했다 (스펙 문서 "기술 검증" 절 참고). 이보다 낮은 버전은 검증되지 않았으므로 이 하한을 지킨다.
- Spark를 쓰는 함수는 `spark`를 매개변수로 받아 pytest에서 로컬 Spark 세션으로 직접 테스트할 수 있게 작성한다 (`src/tlc/gold.py` / `tests/tlc/test_gold.py`의 기존 관례).
- Airflow `@task` 데코레이터가 붙은 함수 자체는 이 프로젝트에서 단위 테스트 대상으로 삼지 않는다 (기존 `store_bronze`, `build_silver`도 테스트되지 않음). 대신 그 안의 순수 로직을 별도 함수로 분리해 그 함수를 테스트한다.
- 새 로그는 기존 관례대로 `src.common.logger.get_logger(__name__, log_to_file=True, log_file_stem=...)`로 생성한다.

---

## 파일 구조

```
requirements.txt                     # 수정: great_expectations 추가
src/common/gx.py                     # 신규: GX 공통 러너
src/common/alerts.py                 # 수정: notify_slack_message() 추가 (리팩터링)
src/tlc/expectations.py              # 신규: taxi_type별 Expectation 정의
src/tlc/bronze_validation.py         # 신규: 파일/청크 검증 로직 + Airflow Task
dags/tlc_pipeline.py                 # 수정: 검증 Task 삽입
tests/common/test_gx.py              # 신규
tests/common/test_alerts.py          # 신규
tests/tlc/test_expectations.py       # 신규
tests/tlc/test_bronze_validation.py  # 신규
```

---

### Task 1: `src/common/gx.py` 공통 러너 + 의존성 추가

**Files:**
- Modify: `requirements.txt`
- Create: `src/common/gx.py`
- Test: `tests/common/test_gx.py`

**Interfaces:**
- Produces: `validate_spark_dataframe(df: pyspark.sql.DataFrame, expectations: list, datasource_name: str, asset_name: str) -> list[dict]`. 반환 리스트의 각 dict는 입력 `expectations` 순서를 유지하며 키 `"success"`(bool), `"expectation_type"`(str), `"kwargs"`(dict), `"result"`(dict)를 가진다.

- [ ] **Step 1: `requirements.txt`에 의존성 추가**

`requirements.txt`의 `pyarrow==25.0.1` / `python-dotenv==1.2.2` / `pytest==8.4.2` 블록 바로 뒤, `fastapi` 블록 앞에 다음을 추가한다:

```

# TLC Bronze 데이터 품질 검증(Great Expectations)에 사용. 1.20.0 미만은
# pyspark==4.0.4와의 호환성을 검증하지 않았으므로 이 하한을 지킨다.
great_expectations>=1.20.0
```

- [ ] **Step 2: 설치**

Run: `pip install -r requirements.txt`
Expected: `great_expectations`가 설치되고 에러 없이 종료.

- [ ] **Step 3: 실패하는 테스트 작성**

`tests/common/test_gx.py`:

```python
import great_expectations as gx
import pytest
from pyspark.sql import SparkSession

from src.common.gx import validate_spark_dataframe


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("gx_test").getOrCreate()
    yield session
    session.stop()


def test_validate_spark_dataframe_detects_null(spark):
    df = spark.createDataFrame(
        [(1, "yellow"), (2, "green"), (3, None)],
        ["trip_id", "taxi_type"],
    )

    results = validate_spark_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_ds",
        asset_name="test_asset",
    )

    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["expectation_type"] == "expect_column_values_to_not_be_null"
    assert results[0]["result"]["unexpected_count"] == 1


def test_validate_spark_dataframe_all_pass(spark):
    df = spark.createDataFrame([(1, "yellow"), (2, "green")], ["trip_id", "taxi_type"])

    results = validate_spark_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_ds2",
        asset_name="test_asset2",
    )

    assert results[0]["success"] is True


def test_validate_spark_dataframe_runs_multiple_expectations_in_order(spark):
    df = spark.createDataFrame([(1,)], ["id"])

    results = validate_spark_dataframe(
        df,
        [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
            gx.expectations.ExpectColumnToExist(column="does_not_exist"),
        ],
        datasource_name="test_ds3",
        asset_name="test_asset3",
    )

    assert len(results) == 2
    assert results[0]["expectation_type"] == "expect_table_row_count_to_be_between"
    assert results[0]["success"] is True
    assert results[1]["expectation_type"] == "expect_column_to_exist"
    assert results[1]["success"] is False
```

- [ ] **Step 4: 테스트 실행해서 실패 확인**

Run: `pytest tests/common/test_gx.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.common.gx'`

- [ ] **Step 5: `src/common/gx.py` 구현**

```python
"""GX(Great Expectations) 공통 러너.

Spark DataFrame과 Expectation 목록을 받아 검증을 실행하고, 결과를 dict
리스트로 반환하는 것까지만 책임진다. 검증 실패 시 어떻게 반응할지
(파일 제외/로그/알림)는 호출하는 도메인 코드가 결정한다.
"""

import great_expectations as gx
from pyspark.sql import DataFrame


def validate_spark_dataframe(
    df: DataFrame,
    expectations: list,
    datasource_name: str,
    asset_name: str,
) -> list[dict]:
    """Spark DataFrame을 Expectation 목록으로 검증한다.

    매 호출마다 새 ephemeral GX Context를 만들어 쓰고 버리므로,
    datasource_name/asset_name은 이 호출 안에서만 고유하면 된다.
    """

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{asset_name}_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    results = []
    for expectation in expectations:
        validation_result = batch.validate(expectation)
        results.append({
            "success": validation_result.success,
            "expectation_type": validation_result.expectation_config.type,
            "kwargs": dict(validation_result.expectation_config.kwargs),
            "result": dict(validation_result.result),
        })
    return results
```

- [ ] **Step 6: 테스트 실행해서 통과 확인**

Run: `pytest tests/common/test_gx.py -v`
Expected: PASS (3 passed)

- [ ] **Step 7: 커밋**

```bash
git add requirements.txt src/common/gx.py tests/common/test_gx.py
git commit -m "feat: GX 공통 검증 러너 추가"
```

---

### Task 2: `src/common/alerts.py`에 `notify_slack_message()` 추가

**Files:**
- Modify: `src/common/alerts.py`
- Test: `tests/common/test_alerts.py`

**Interfaces:**
- Consumes: 없음 (Task 1과 독립).
- Produces: `notify_slack_message(text: str) -> None`. 기존 `notify_slack_failure(context: dict) -> None`의 동작(웹훅 없으면 경고 로그 후 종료, 전송 실패해도 예외를 던지지 않음)은 그대로 유지된다.

**배경:** `notify_slack_failure`는 Airflow `on_failure_callback`(Task 최종 실패 시에만 호출)용이다. Bronze 검증은 파일 하나를 제외해도 Task 자체는 성공하므로, Airflow 실패 콜백과 무관하게 즉시 Slack을 보낼 범용 함수가 필요하다. 웹훅 POST 로직이 두 함수에 중복되지 않도록 `_post_to_slack()`으로 추출한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/common/test_alerts.py`:

```python
from unittest.mock import MagicMock, patch

from src.common import alerts


def test_notify_slack_message_posts_to_webhook(monkeypatch):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    with patch.object(alerts.requests, "post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        alerts.notify_slack_message("테스트 메시지")

    mock_post.assert_called_once_with(
        "https://hooks.slack.test/webhook",
        json={"text": "테스트 메시지"},
        timeout=alerts.SLACK_TIMEOUT,
    )


def test_notify_slack_message_skips_when_webhook_missing(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", None)

    with patch.object(alerts.requests, "post") as mock_post:
        with caplog.at_level("WARNING"):
            alerts.notify_slack_message("테스트 메시지")

    mock_post.assert_not_called()
    assert any("SLACK_WEBHOOK_URL" in rec.message for rec in caplog.records)


def test_notify_slack_message_swallows_request_exception(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    with patch.object(
        alerts.requests, "post",
        side_effect=alerts.requests.exceptions.ConnectionError("boom"),
    ):
        with caplog.at_level("ERROR"):
            alerts.notify_slack_message("테스트 메시지")  # 예외를 던지면 안 됨

    assert any("전송 실패" in rec.message for rec in caplog.records)


def test_notify_slack_failure_still_works_after_refactor(monkeypatch):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    context = {
        "task_instance": MagicMock(
            dag_id="tlc_pipeline", task_id="store_bronze",
            try_number=3, log_url="http://example.com/log",
        ),
        "exception": ValueError("boom"),
        "logical_date": "2026-08-18",
    }

    with patch.object(alerts.requests, "post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        alerts.notify_slack_failure(context)

    assert mock_post.call_count == 1
    sent_text = mock_post.call_args.kwargs["json"]["text"]
    assert "tlc_pipeline" in sent_text
    assert "store_bronze" in sent_text
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/common/test_alerts.py -v`
Expected: FAIL with `AttributeError: module 'src.common.alerts' has no attribute 'notify_slack_message'`

- [ ] **Step 3: `src/common/alerts.py` 리팩터링 + 함수 추가**

`_build_message` 함수 정의 다음, 기존 `def notify_slack_failure(context: dict) -> None:` 함수 전체를 아래로 교체한다:

```python
def _post_to_slack(text: str) -> None:
    """Slack Webhook으로 텍스트를 전송한다.

    알림 자체가 실패해도(webhook 오류, 네트워크 문제 등) 호출부의 원래
    처리 흐름을 가리면 안 되므로, 여기서 발생하는 예외는 밖으로 던지지
    않고 로그만 남긴다.
    """

    if not SLACK_WEBHOOK_URL:
        logger.warning(
            "SLACK_WEBHOOK_URL이 없어서 알림을 건너뜁니다 — .env 확인"
        )
        return

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=SLACK_TIMEOUT,
        )
        response.raise_for_status()

        logger.info("Slack 메시지 전송 완료")

    except Exception:
        logger.exception("Slack 메시지 전송 실패")


def notify_slack_failure(context: dict) -> None:
    """on_failure_callback으로 등록해서 쓰는 함수."""

    _post_to_slack(_build_message(context))


def notify_slack_message(text: str) -> None:
    """Airflow 실패 콜백과 무관하게, 임의의 텍스트를 Slack으로 즉시 전송한다.

    Bronze 검증처럼 Task 자체는 성공하지만 특정 파일을 제외했다는 걸
    바로 알려야 할 때 쓴다 — on_failure_callback은 Task가 최종 실패로
    확정될 때만 호출되므로 이런 경우엔 발동하지 않는다.
    """

    _post_to_slack(text)
```

또한 파일 최상단 docstring의 마지막 문단(`알림 자체가 실패해도...` 로 시작하는 문단)은 이제 `_post_to_slack`의 docstring으로 옮겨졌으므로, 모듈 docstring에는 아래 문장을 추가해 새 함수도 안내한다 — 기존 `DAG의 default_args에 이렇게 걸어서 쓴다:` 예시 코드 블록 바로 다음, 파일 마지막 문단 앞에 삽입:

```

Task 자체는 성공하지만 특정 파일만 제외하는 등, Task 최종 실패가 아닌
상황에서 즉시 알림이 필요하면 notify_slack_message(text)를 직접 호출한다.
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/common/test_alerts.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/common/alerts.py tests/common/test_alerts.py
git commit -m "refactor: alerts.py에 notify_slack_message 추가, POST 로직 공통화"
```

---

### Task 3: `src/tlc/expectations.py` — taxi_type별 Expectation 정의

**Files:**
- Create: `src/tlc/expectations.py`
- Test: `tests/tlc/test_expectations.py`

**Interfaces:**
- Consumes: `src.tlc.transform.COLUMN_MAPPING` (기존, 변경 없음), `great_expectations`(Task 1에서 설치).
- Produces:
  - `CRITICAL_COLUMNS: list[str]` — `["dropoff_datetime", "dropoff_location_id"]`
  - `critical_expectations(taxi_type: str) -> list` — `ExpectColumnToExist` 목록.
  - `log_only_expectations(taxi_type: str) -> list` — row count/존재/not-null/범위 검증 목록.

**컬럼 매핑 원천:** `transform.py`의 `COLUMN_MAPPING`(원본 컬럼명 → Silver 컬럼명)을 그대로 뒤집어 재사용한다 — taxi_type별 컬럼 구성을 두 파일에 따로 하드코딩하면 나중에 어긋날 수 있다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_expectations.py`:

```python
import great_expectations as gx

from src.tlc.expectations import critical_expectations, log_only_expectations


def test_critical_expectations_yellow_checks_dropoff_columns():
    expectations = critical_expectations("yellow")

    columns = {e.column for e in expectations}
    assert columns == {"tpep_dropoff_datetime", "DOLocationID"}
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_critical_expectations_fhv_uses_fhv_column_names():
    expectations = critical_expectations("fhv")

    columns = {e.column for e in expectations}
    assert columns == {"dropOff_datetime", "DOlocationID"}


def test_log_only_expectations_yellow_includes_passenger_and_distance_checks():
    expectations = log_only_expectations("yellow")

    types_and_columns = {
        (type(e).__name__, getattr(e, "column", None)) for e in expectations
    }
    assert ("ExpectColumnValuesToBeBetween", "passenger_count") in types_and_columns
    assert ("ExpectColumnValuesToBeBetween", "trip_distance") in types_and_columns


def test_log_only_expectations_fhv_excludes_passenger_and_distance_checks():
    expectations = log_only_expectations("fhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "passenger_count" not in columns
    assert "trip_distance" not in columns


def test_log_only_expectations_fhvhv_checks_trip_miles_as_trip_distance():
    expectations = log_only_expectations("fhvhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "trip_miles" in columns
    assert "passenger_count" not in columns


def test_log_only_expectations_includes_row_count_check():
    expectations = log_only_expectations("yellow")

    assert any(
        isinstance(e, gx.expectations.ExpectTableRowCountToBeBetween)
        for e in expectations
    )
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/tlc/test_expectations.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.tlc.expectations'`

- [ ] **Step 3: `src/tlc/expectations.py` 구현**

```python
"""TLC Bronze taxi_type별 Great Expectations 정의.

컬럼 구성은 src.tlc.transform.COLUMN_MAPPING(원본 컬럼명 → Silver 컬럼명)을
뒤집어 재사용한다 — taxi_type별 컬럼 구성을 여기 따로 하드코딩하면
transform.py와 어긋날 위험이 있다.
"""

import great_expectations as gx

from src.tlc.transform import COLUMN_MAPPING


# dropoff_datetime/dropoff_location_id는 Silver의 traffic score 분석(세그먼트별
# 하차 위치·시각 집계)에 직접 쓰이는 핵심 값이라, 원본 컬럼 자체가 없으면
# critical로 다룬다. 그 외 컬럼이 없거나 값이 이상한 경우는 로그만 남긴다.
CRITICAL_COLUMNS = ["dropoff_datetime", "dropoff_location_id"]


def _raw_columns(taxi_type: str) -> dict:
    """taxi_type의 Silver 컬럼명 → 원본 컬럼명 매핑 (COLUMN_MAPPING의 역방향)."""

    return {
        silver_name: raw_name
        for raw_name, silver_name in COLUMN_MAPPING[taxi_type].items()
    }


def critical_expectations(taxi_type: str) -> list:
    """실패 시 파일을 Silver로 넘기지 않고 제외해야 하는 검증."""

    columns = _raw_columns(taxi_type)
    return [
        gx.expectations.ExpectColumnToExist(column=columns[name])
        for name in CRITICAL_COLUMNS
    ]


def log_only_expectations(taxi_type: str) -> list:
    """실패해도 로그만 남기고 파일은 계속 Silver로 진행하는 검증.

    passenger_count/trip_distance는 taxi_type에 따라 원본에 아예 없을 수
    있으므로(COLUMN_MAPPING 참고), 그 taxi_type에 실제로 존재하는 컬럼에
    대해서만 검증을 추가한다.
    """

    columns = _raw_columns(taxi_type)

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        gx.expectations.ExpectColumnToExist(column=columns["pickup_datetime"]),
        gx.expectations.ExpectColumnToExist(column=columns["pickup_location_id"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["pickup_datetime"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["dropoff_datetime"]),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["pickup_location_id"], min_value=1, max_value=263,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["dropoff_location_id"], min_value=1, max_value=263,
        ),
    ]

    if "passenger_count" in columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=columns["passenger_count"], min_value=0, max_value=None,
            )
        )

    if "trip_distance" in columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=columns["trip_distance"], min_value=0, max_value=None,
            )
        )

    return expectations
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/tlc/test_expectations.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/expectations.py tests/tlc/test_expectations.py
git commit -m "feat: TLC Bronze taxi_type별 Expectation 정의 추가"
```

---

### Task 4: `src/tlc/bronze_validation.py` — 파일 단위 검증

**Files:**
- Create: `src/tlc/bronze_validation.py`
- Test: `tests/tlc/test_bronze_validation.py`

**Interfaces:**
- Consumes: `validate_spark_dataframe`(Task 1), `critical_expectations`/`log_only_expectations`(Task 3).
- Produces: `CriticalValidationError`(Exception 서브클래스), `validate_bronze_file(spark, bronze_path: str, taxi_type: str) -> list[dict]` — critical 실패 시 `CriticalValidationError`를 던진다. 통과하면 log-only 검증 중 실패한 항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/tlc/test_bronze_validation.py`:

```python
from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.tlc.bronze_validation import CriticalValidationError, validate_bronze_file


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("bronze_validation_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_bronze_fixture(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_parquet(
        path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True,
    )
    return str(path)


def test_validate_bronze_file_passes_clean_yellow_file(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "good.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    failed_checks = validate_bronze_file(spark, path, "yellow")

    assert failed_checks == []


def test_validate_bronze_file_raises_when_dropoff_column_missing(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "critical.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        # tpep_dropoff_datetime 컬럼 자체가 없음
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    with pytest.raises(CriticalValidationError, match="tpep_dropoff_datetime"):
        validate_bronze_file(spark, path, "yellow")


def test_validate_bronze_file_logs_but_passes_when_location_out_of_range(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "out_of_range.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 999,  # 유효 범위(1~263) 밖
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    failed_checks = validate_bronze_file(spark, path, "yellow")

    assert len(failed_checks) == 1
    assert failed_checks[0]["kwargs"]["column"] == "PULocationID"


def test_validate_bronze_file_fhv_skips_passenger_count_check(tmp_path, spark):
    # FHV는 passenger_count/trip_distance 컬럼 자체가 없는 게 정상이다.
    path = _write_bronze_fixture(tmp_path, "fhv.parquet", [{
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropOff_datetime": datetime(2024, 1, 1, 8, 30),
        "PUlocationID": 10,
        "DOlocationID": 20,
    }])

    failed_checks = validate_bronze_file(spark, path, "fhv")

    assert failed_checks == []
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/tlc/test_bronze_validation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.tlc.bronze_validation'`

- [ ] **Step 3: `src/tlc/bronze_validation.py` 구현 (Task 4 범위분)**

```python
"""TLC Bronze 데이터 품질 검증 (Great Expectations 기반).

같은 taxi_type 파일들을 Spark 세션 하나로 순회하며 검증하되(build_silver와
동일한 세션 재사용 패턴), 파일 하나의 검증 실패가 같은 청크의 다른 파일까지
막지 않도록 파일 단위로 개별 판정한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.alerts import notify_slack_message
from src.common.gx import validate_spark_dataframe
from src.common.logger import get_logger
from src.common.spark import get_spark

from src.tlc.expectations import critical_expectations, log_only_expectations


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze_validation")


class CriticalValidationError(Exception):
    """Bronze 파일의 critical 검증(필수 컬럼 존재)이 실패했을 때 발생한다."""


def validate_bronze_file(spark, bronze_path: str, taxi_type: str) -> list[dict]:
    """Bronze 파일 하나를 검증한다.

    critical 검증(dropoff_datetime/dropoff_location_id 원본 컬럼 존재)이
    실패하면 CriticalValidationError를 던진다. 통과하면 log-only 검증 중
    실패한 항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    df = spark.read.parquet(str(bronze_path))
    asset_id = Path(bronze_path).stem

    critical_results = validate_spark_dataframe(
        df,
        critical_expectations(taxi_type),
        datasource_name=f"tlc_bronze_critical_{asset_id}",
        asset_name=f"tlc_bronze_critical_{asset_id}",
    )
    failed_critical = [r for r in critical_results if not r["success"]]
    if failed_critical:
        missing_columns = [r["kwargs"].get("column") for r in failed_critical]
        raise CriticalValidationError(
            f"필수 컬럼 없음: {missing_columns} (taxi_type={taxi_type})"
        )

    log_results = validate_spark_dataframe(
        df,
        log_only_expectations(taxi_type),
        datasource_name=f"tlc_bronze_logonly_{asset_id}",
        asset_name=f"tlc_bronze_logonly_{asset_id}",
    )
    return [r for r in log_results if not r["success"]]
```

`@task`, `notify_slack_message`, `get_spark` 임포트는 이 Task에서는 아직 쓰이지 않지만 Task 5에서 같은 파일에 이어서 쓰인다 — 지금 추가해도 무해하다(미사용 임포트 없음, Task 5 완료 시점에 전부 사용됨).

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/tlc/test_bronze_validation.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/bronze_validation.py tests/tlc/test_bronze_validation.py
git commit -m "feat: TLC Bronze 파일 단위 GX 검증 함수 추가"
```

---

### Task 5: 청크 순회 + 개별 판정 + Airflow Task

**Files:**
- Modify: `src/tlc/bronze_validation.py` (Task 4 파일에 이어서 작성)
- Modify: `tests/tlc/test_bronze_validation.py` (Task 4 파일에 이어서 작성)

**Interfaces:**
- Consumes: `validate_bronze_file`, `CriticalValidationError`(Task 4), `notify_slack_message`(Task 2), `get_spark`(기존 `src/common/spark.py`).
- Produces: `_validate_chunk_files(spark, bronze_chunk: list[dict]) -> list[dict]`, Airflow Task `validate_bronze_quality(bronze_chunk: list[dict]) -> list[dict]` (통과한 파일의 dict만 담긴 리스트, 입력과 동일한 dict 형태 유지).

- [ ] **Step 1: 실패하는 테스트 추가**

`tests/tlc/test_bronze_validation.py` 맨 위 import 블록에 추가:

```python
from unittest.mock import patch

from src.tlc import bronze_validation
```

파일 맨 아래에 추가:

```python
def test_validate_chunk_files_excludes_only_critical_failure(tmp_path, spark):
    good_path = _write_bronze_fixture(tmp_path, "good.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    critical_path = _write_bronze_fixture(tmp_path, "critical2.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])

    chunk = [
        {"filename": "good.parquet", "taxi_type": "yellow", "bronze_path": good_path},
        {"filename": "critical2.parquet", "taxi_type": "yellow", "bronze_path": critical_path},
    ]

    with patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        passed = bronze_validation._validate_chunk_files(spark, chunk)

    assert [f["filename"] for f in passed] == ["good.parquet"]
    mock_notify.assert_called_once()
    assert "critical2.parquet" in mock_notify.call_args.args[0]


def test_validate_chunk_files_empty_chunk_returns_empty(spark):
    assert bronze_validation._validate_chunk_files(spark, []) == []
```

- [ ] **Step 2: 테스트 실행해서 실패 확인**

Run: `pytest tests/tlc/test_bronze_validation.py -v -k validate_chunk_files`
Expected: FAIL with `AttributeError: module 'src.tlc.bronze_validation' has no attribute '_validate_chunk_files'`

- [ ] **Step 3: `src/tlc/bronze_validation.py`에 이어서 구현**

`validate_bronze_file` 함수 정의 다음에 추가:

```python
def _validate_chunk_files(spark, bronze_chunk: list[dict]) -> list[dict]:
    """청크(taxi_type 하나) 안 파일을 순회하며 개별 판정한다.

    한 파일의 예외가 루프 밖으로 전파되지 않게 파일마다 개별 try/except로
    감싸서, critical 실패 파일만 결과에서 제외되고 나머지는 계속 처리된다.
    """

    passed = []

    for bronze_result in bronze_chunk:
        filename = bronze_result["filename"]
        taxi_type = bronze_result["taxi_type"]
        bronze_path = bronze_result["bronze_path"]

        try:
            failed_checks = validate_bronze_file(spark, bronze_path, taxi_type)

            for check in failed_checks:
                logger.warning(
                    f"검증 실패(로그만) - {filename} : "
                    f"{check['expectation_type']} {check['kwargs']} → {check['result']}"
                )

            passed.append(bronze_result)

        except CriticalValidationError as error:
            logger.error(f"Critical 검증 실패 - {filename} : {error}")
            notify_slack_message(
                f":warning: TLC Bronze 검증 실패로 파일 제외\n"
                f"*파일*: `{filename}`\n*사유*: {error}"
            )

        except Exception as error:
            logger.error(f"Bronze 파일 검증 중 오류 - {filename} : {error}")
            notify_slack_message(
                f":warning: TLC Bronze 검증 중 오류로 파일 제외\n"
                f"*파일*: `{filename}`\n*사유*: {error}"
            )

    return passed


@task(pool="silver_pool")
def validate_bronze_quality(bronze_chunk: list[dict]) -> list[dict]:
    """청크(taxi_type 하나) 안 파일들을 검증하고, 통과한 파일만 반환한다.

    build_silver와 같은 이유로 taxi_type당 Spark 세션 하나를 재사용하고
    같은 silver_pool을 공유한다 — spark-worker가 1대(10코어)뿐인 유한
    자원이라, Bronze 검증과 Silver 변환이 각자 다른 풀로 동시에 실행되면
    풀 슬롯 상한(3개)과 무관하게 Spark 클러스터 코어가 초과 예약될 수 있다.
    """

    if not bronze_chunk:
        return []

    spark = get_spark()

    try:
        return _validate_chunk_files(spark, bronze_chunk)
    finally:
        spark.stop()
```

- [ ] **Step 4: 테스트 실행해서 통과 확인**

Run: `pytest tests/tlc/test_bronze_validation.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: 커밋**

```bash
git add src/tlc/bronze_validation.py tests/tlc/test_bronze_validation.py
git commit -m "feat: Bronze 청크 검증 Airflow Task 추가 (파일별 개별 격리)"
```

---

### Task 6: `dags/tlc_pipeline.py`에 검증 Task 연결

**Files:**
- Modify: `dags/tlc_pipeline.py`

**Interfaces:**
- Consumes: `validate_bronze_quality`(Task 5).
- Produces: 없음 (DAG 배선 변경, 파이프라인 최종 진입점).

- [ ] **Step 1: import 추가**

`dags/tlc_pipeline.py`의 기존:

```python
from src.tlc.silver import (
    build_silver,
    chunk_bronze_files,
)
```

바로 다음에 추가:

```python

from src.tlc.bronze_validation import (
    validate_bronze_quality,
)
```

- [ ] **Step 2: 모듈 docstring 갱신**

파일 최상단 docstring의:

```
Download
    ↓
Validate
    ↓
Bronze
    ↓
Silver
```

를 다음으로 교체:

```
Download
    ↓
Validate
    ↓
Bronze
    ↓
Validate (Great Expectations)
    ↓
Silver
```

- [ ] **Step 3: DAG 본문에 Task 삽입**

기존:

```python
    # -----------------------------------------
    # 5. taxi_type별 청크로 묶기
    # -----------------------------------------

    bronze_chunks = chunk_bronze_files(
        bronze_files=bronze_files,
    )

    # -----------------------------------------
    # 6. 청크별 Silver 변환 (청크당 Spark 세션 1개)
    # -----------------------------------------

    build_silver.expand(
        bronze_chunk=bronze_chunks,
    )
```

를 다음으로 교체:

```python
    # -----------------------------------------
    # 5. taxi_type별 청크로 묶기
    # -----------------------------------------

    bronze_chunks = chunk_bronze_files(
        bronze_files=bronze_files,
    )

    # -----------------------------------------
    # 6. 청크별 Bronze 데이터 품질 검증 (Great Expectations)
    # -----------------------------------------

    validated_chunks = validate_bronze_quality.expand(
        bronze_chunk=bronze_chunks,
    )

    # -----------------------------------------
    # 7. 청크별 Silver 변환 (청크당 Spark 세션 1개)
    # -----------------------------------------

    build_silver.expand(
        bronze_chunk=validated_chunks,
    )
```

- [ ] **Step 4: 문법 확인**

Run: `python -m py_compile dags/tlc_pipeline.py`
Expected: 에러 없이 종료(exit code 0). 이 프로젝트는 DAG를 pytest로 단위 테스트하는 기존 관례가 없으므로(다른 태스크들도 동일), 이 구문 검증 + 아래 Step 5의 수동 확인으로 대체한다.

- [ ] **Step 5: (환경이 준비되어 있다면) Airflow DAG import 검증**

Docker 환경이 떠 있다면:

Run: `docker compose exec airflow-worker airflow dags list-import-errors`
Expected: `tlc_pipeline` 관련 항목 없음(빈 목록 또는 다른 DAG만 표시).

환경이 없다면 이 단계는 건너뛰고 Step 4의 구문 검증과 코드 리뷰로 갈음한다.

- [ ] **Step 6: 커밋**

```bash
git add dags/tlc_pipeline.py
git commit -m "feat: tlc_pipeline에 Bronze GX 검증 Task 연결"
```

---

## 최종 확인

- [ ] 전체 테스트 스위트 실행

Run: `pytest tests/ -v`
Expected: 모든 테스트 PASS (기존 `tests/tlc/test_gold.py` 포함, 총 4개 신규 파일 + 기존 파일 전부 통과).

- [ ] `docs/superpowers/specs/2026-08-18-tlc-bronze-great-expectations-design.md`의 "범위 → 포함" 항목과 이 플랜의 Task 1~6을 다시 대조해 빠진 항목이 없는지 확인 (아래 "Self-Review" 참고).
