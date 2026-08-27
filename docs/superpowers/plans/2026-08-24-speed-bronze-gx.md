# speed Bronze GX 검증 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `segment_time_pipeline`이 NYC DOT 실시간 속도 API에서 받아온 Bronze 데이터를 EMR에 넘기기 전에 Great Expectations로 검증해서, 컬럼이 사라지는 스키마 변경과 값만 조용히 비는 데이터 품질 저하를 각각 다르게 대응한다.

**Architecture:** TLC의 critical/log_only 2단 검증 패턴(`src/tlc/expectations.py` + `src/tlc/bronze_validation.py`)을 그대로 따르되, speed 전용 모듈로 새로 만든다. Bronze 파일이 30분당 1개뿐이라 TLC처럼 "파일 제외"가 아니라 "이번 사이클 스킵"으로 critical 실패에 대응한다. `@task.short_circuit`로 EMR 제출 전에 게이트를 건다.

**Tech Stack:** `src/common/gx.py`의 `validate_pandas_dataframe`(pandas 기반, Spark 세션 불필요), Airflow `@task.short_circuit`, `src/common/alerts.py`의 `notify_slack_message`.

## Global Constraints

- 이 작업 범위는 **speed Bronze만**이다. Silver1(EMR job 내부)은 범위 밖.
- 새 검증은 `collect_bronze()` 직후, `submit_nav_time_job()`(EMR 제출) 이전에 실행한다.
- critical 검증 대상은 다운스트림(`clean_speed_silver1`)이 실제로 쓰는 4개 컬럼만: `speed`, `link_points`, `data_as_of`, `link_id`.
- critical 실패 → `short_circuit`으로 이번 사이클 스킵(EMR 제출 안 함), Slack 알림을 **직접** 호출(`on_failure_callback`은 short_circuit 스킵엔 안 걸림).
- log_only 실패 → 배치 처리는 계속(EMR 제출함), 로그 + Slack 알림 둘 다 보낸다(TLC는 log_only에 Slack을 안 보내는데 speed는 의도적으로 다르게 감).
- null 체크는 컬럼별로 독립적으로 `mostly=0.90`(10% 넘게 비면 걸림) 적용.
- `speed` 값 범위: 0~150. `data_as_of` 날짜 범위: 2017-01-01 ~ 검증 실행 시점 기준 오늘+1일.
- **`speed`/`data_as_of`는 Bronze parquet에 문자열로 저장되어 있다**(Socrata가 모든 필드를 문자열로 주기 때문 — 실제 프로덕션 파일로 확인함). `ExpectColumnValuesToBeBetween`을 문자열 컬럼에 그대로 돌리면 GX가 타입 불일치 예외를 내부적으로 삼켜서 `success=False, result={}`만 남긴다(실제로 재현 확인함). 검증 직전에 **원본과 분리된 복사본**에서만 `pd.to_numeric`/`pd.to_datetime`(둘 다 `errors="coerce"`)으로 캐스팅한다 — 원본 Bronze 파일은 그대로 둔다.

---

### Task 1: speed Bronze expectation 정의

**Files:**
- Create: `src/speed/expectations.py`
- Test: `tests/speed/test_expectations.py`

**Interfaces:**
- Produces: `critical_expectations() -> list` (4개 `ExpectColumnToExist`), `log_only_expectations() -> list` (7개 expectation, `data_as_of`/`speed` 범위 포함)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/speed/test_expectations.py`:

```python
from datetime import datetime, timedelta

import great_expectations as gx

from src.speed.expectations import critical_expectations, log_only_expectations


def test_critical_expectations_checks_downstream_columns():
    expectations = critical_expectations()

    columns = {e.column for e in expectations}
    assert columns == {"speed", "link_points", "data_as_of", "link_id"}
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_log_only_expectations_includes_row_count_check():
    expectations = log_only_expectations()

    types = [type(e).__name__ for e in expectations]
    assert "ExpectTableRowCountToBeBetween" in types


def test_log_only_expectations_null_checks_use_ten_percent_tolerance():
    expectations = log_only_expectations()

    null_checks = {
        e.column: e.mostly
        for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToNotBeNull"
    }
    assert null_checks == {
        "speed": 0.90,
        "link_points": 0.90,
        "data_as_of": 0.90,
        "link_id": 0.90,
    }


def test_log_only_expectations_speed_range_is_zero_to_150():
    expectations = log_only_expectations()

    speed_range = next(
        e for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToBeBetween" and e.column == "speed"
    )
    assert speed_range.min_value == 0
    assert speed_range.max_value == 150


def test_log_only_expectations_data_as_of_range_starts_2017_and_ends_near_now():
    expectations = log_only_expectations()

    date_range = next(
        e for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToBeBetween" and e.column == "data_as_of"
    )
    assert date_range.min_value == datetime(2017, 1, 1)

    # max_value는 호출 시점 기준으로 동적 계산되므로 정확한 값이 아니라
    # "오늘+1일 근방"인지만 확인한다.
    expected_max = datetime.now() + timedelta(days=1)
    assert abs((date_range.max_value - expected_max).total_seconds()) < 60
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/speed/test_expectations.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.speed.expectations'`

- [ ] **Step 3: 최소 구현 작성**

`src/speed/expectations.py`:

```python
"""speed Bronze Great Expectations 정의.

TLC(`src/tlc/expectations.py`)와 같은 critical/log_only 2단 구조를
따르되, speed는 taxi_type 같은 분기가 없어서 목록이 고정이다.
"""

from datetime import datetime, timedelta

import great_expectations as gx

# 다운스트림(clean_speed_silver1)이 실제로 참조하는 컬럼만 critical로
# 잡는다 - API가 주는 나머지 9개 컬럼(status/owner/borough 등)은 우리
# 파이프라인이 안 쓰므로 사라져도 critical이 아니다.
_REQUIRED_COLUMNS = ["speed", "link_points", "data_as_of", "link_id"]

# 개별 센서의 산발적 결측(노이즈)은 넘기고, 스키마 드리프트처럼 값이
# 뭉텅이로 비는 경우만 잡기 위한 허용치.
_NULL_TOLERANCE = 0.90

# 이 데이터셋 생성일자(Socrata 메타데이터 createdAt=2017-04-17) 기준.
_DATA_AS_OF_MIN = datetime(2017, 1, 1)


def critical_expectations() -> list:
    """실패 시 이번 파이프라인 사이클을 스킵해야 하는 검증.

    컬럼이 실제로 사라졌는지(존재 여부)만 본다 - 값 이상은
    log_only_expectations로 다룬다.
    """

    return [
        gx.expectations.ExpectColumnToExist(column=column)
        for column in _REQUIRED_COLUMNS
    ]


def log_only_expectations() -> list:
    """실패해도 배치 처리는 계속하되 Slack 알림을 보내는 검증.

    컬럼은 있지만 값이 이상한 경우(null 급증, 범위 이탈)를 잡는다.
    """

    return [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        *[
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column, mostly=_NULL_TOLERANCE)
            for column in _REQUIRED_COLUMNS
        ],
        gx.expectations.ExpectColumnValuesToBeBetween(column="speed", min_value=0, max_value=150),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="data_as_of",
            min_value=_DATA_AS_OF_MIN,
            max_value=datetime.now() + timedelta(days=1),
        ),
    ]
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/speed/test_expectations.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: 커밋**

```bash
git add src/speed/expectations.py tests/speed/test_expectations.py
git commit -m "feat: speed Bronze GX expectation 정의 추가"
```

---

### Task 2: speed Bronze 파일 검증 함수

**Files:**
- Create: `src/speed/bronze_validation.py`
- Test: `tests/speed/test_bronze_validation.py`

**Interfaces:**
- Consumes: Task 1의 `critical_expectations()`, `log_only_expectations()`; `src/common/gx.py`의 `validate_pandas_dataframe(df, expectations, datasource_name, asset_name) -> list[dict]`
- Produces: `CriticalValidationError`(Exception 서브클래스), `validate_bronze_file(bronze_path: str) -> list[dict]`(critical 실패 시 raise, 통과하면 log_only 실패 목록 반환)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/speed/test_bronze_validation.py`:

```python
import pandas as pd
import pytest

from src.speed import bronze_validation
from src.speed.bronze_validation import CriticalValidationError, validate_bronze_file


def _write_bronze_fixture(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_parquet(path, index=False)
    return str(path)


def _good_row(**overrides):
    row = {
        "id": "1", "speed": "29.82", "travel_time": "90", "status": "0",
        "data_as_of": "2026-08-23T02:55:08.000", "link_id": "4620332",
        "link_points": "40.0,-73.0", "encoded_poly_line": "", "encoded_poly_line_lvls": "",
        "owner": "NYC-DOT", "transcom_id": "4620332", "borough": "Manhattan",
        "link_name": "TEST ST",
    }
    row.update(overrides)
    return row


def test_validate_bronze_file_passes_clean_file(tmp_path):
    path = _write_bronze_fixture(tmp_path, "good.parquet", [_good_row()])

    failed_checks = validate_bronze_file(path)

    assert failed_checks == []


def test_validate_bronze_file_raises_when_speed_column_missing(tmp_path):
    row = _good_row()
    del row["speed"]
    path = _write_bronze_fixture(tmp_path, "critical.parquet", [row])

    with pytest.raises(CriticalValidationError, match="speed"):
        validate_bronze_file(path)


def test_validate_bronze_file_logs_but_passes_when_speed_out_of_range(tmp_path):
    path = _write_bronze_fixture(
        tmp_path, "out_of_range.parquet", [_good_row(speed="-5.0")]
    )

    failed_checks = validate_bronze_file(path)

    assert len(failed_checks) == 1
    assert failed_checks[0]["kwargs"]["column"] == "speed"


def test_validate_bronze_file_flags_ancient_data_as_of(tmp_path):
    # 실제로 라이브 API에서 발견했던 1930년 이상치 재현.
    path = _write_bronze_fixture(
        tmp_path, "ancient.parquet", [_good_row(data_as_of="1930-12-09T14:40:47.000")]
    )

    failed_checks = validate_bronze_file(path)

    assert any(check["kwargs"].get("column") == "data_as_of" for check in failed_checks)


def test_validate_bronze_file_does_not_mutate_original_dtypes(tmp_path):
    # speed/data_as_of 캐스팅은 검증용 복사본에서만 해야 한다 - 원본
    # Bronze 파일 자체가 바뀌면 안 된다(Bronze 원칙: 변환 없음).
    path = _write_bronze_fixture(tmp_path, "good.parquet", [_good_row()])
    before = pd.read_parquet(path)

    validate_bronze_file(path)

    after = pd.read_parquet(path)
    assert before["speed"].dtype == after["speed"].dtype == object


def test_validate_bronze_file_null_within_tolerance_does_not_fail(tmp_path):
    # 10개 중 1개(10%)만 비어있으면 mostly=0.90 허용치 이내라 안 걸려야 한다.
    rows = [_good_row(id=str(i)) for i in range(9)]
    rows.append(_good_row(id="9", speed=None))
    path = _write_bronze_fixture(tmp_path, "mostly_ok.parquet", rows)

    failed_checks = validate_bronze_file(path)

    assert not any(
        c["kwargs"].get("column") == "speed" and c["expectation_type"] == "expect_column_values_to_not_be_null"
        for c in failed_checks
    )


def test_validate_bronze_file_null_over_tolerance_fails(tmp_path):
    # 10개 중 2개(20%)가 비면 mostly=0.90 허용치를 넘어서 걸려야 한다.
    rows = [_good_row(id=str(i)) for i in range(8)]
    rows.append(_good_row(id="8", speed=None))
    rows.append(_good_row(id="9", speed=None))
    path = _write_bronze_fixture(tmp_path, "over_tolerance.parquet", rows)

    failed_checks = validate_bronze_file(path)

    assert any(
        c["kwargs"].get("column") == "speed" and c["expectation_type"] == "expect_column_values_to_not_be_null"
        for c in failed_checks
    )
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/speed/test_bronze_validation.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.speed.bronze_validation'`

- [ ] **Step 3: 최소 구현 작성**

`src/speed/bronze_validation.py`:

```python
"""speed Bronze 데이터 품질 검증 (Great Expectations 기반).

30분마다 수집되는 Bronze 파일 하나를 pandas로 검증한다 - 파일이 작아서
(수백~수천 행) Spark 세션 없이 pandas로 충분하다. TLC(taxi_type × 월,
여러 파일)와 달리 speed는 사이클당 파일이 하나뿐이라 "파일 제외" 대신
"이번 사이클 스킵"으로 critical 실패에 대응한다(DAG 쪽 처리는
Task 3 참고).
"""

from __future__ import annotations

import pandas as pd

from src.common.gx import validate_pandas_dataframe
from src.common.logger import get_logger
from src.speed.expectations import critical_expectations, log_only_expectations

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze_validation")


class CriticalValidationError(Exception):
    """speed Bronze의 critical 검증(필수 컬럼 존재)이 실패했을 때 발생한다."""


def _cast_for_validation(df: pd.DataFrame) -> pd.DataFrame:
    """speed/data_as_of는 Bronze에 문자열로 저장되어 있다(Socrata가 모든
    필드를 문자열로 주기 때문). ExpectColumnValuesToBeBetween을 문자열
    컬럼에 그대로 돌리면 GX가 타입 불일치 예외를 내부적으로 삼켜서
    success=False, result={}만 남긴다(실제로 재현 확인됨). 그래서 검증
    직전에 복사본에서만 캐스팅한다 - 원본 df는 그대로 둔다.

    errors="coerce"로 파싱 안 되는 값은 예외 대신 null로 만든다 - 그러면
    not-null 체크에서 자연스럽게 잡힌다.
    """

    validation_df = df.copy()
    validation_df["speed"] = pd.to_numeric(validation_df["speed"], errors="coerce")
    validation_df["data_as_of"] = pd.to_datetime(validation_df["data_as_of"], errors="coerce")
    return validation_df


def validate_bronze_file(bronze_path: str) -> list[dict]:
    """speed Bronze 파일 하나를 검증한다.

    critical 검증(다운스트림이 의존하는 컬럼 존재 여부)이 실패하면
    CriticalValidationError를 던진다. 통과하면 log-only 검증 중 실패한
    항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    df = pd.read_parquet(bronze_path)

    critical_results = validate_pandas_dataframe(
        df,
        critical_expectations(),
        datasource_name="speed_bronze_critical",
        asset_name="speed_bronze_critical",
    )
    failed_critical = [r for r in critical_results if not r["success"]]
    if failed_critical:
        missing_columns = [r["kwargs"].get("column") for r in failed_critical]
        raise CriticalValidationError(f"필수 컬럼 없음: {missing_columns}")

    validation_df = _cast_for_validation(df)
    log_results = validate_pandas_dataframe(
        validation_df,
        log_only_expectations(),
        datasource_name="speed_bronze_logonly",
        asset_name="speed_bronze_logonly",
    )
    return [r for r in log_results if not r["success"]]
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/speed/test_bronze_validation.py -v`
Expected: PASS (7 tests)

- [ ] **Step 5: 커밋**

```bash
git add src/speed/bronze_validation.py tests/speed/test_bronze_validation.py
git commit -m "feat: speed Bronze 파일 GX 검증 함수 추가"
```

---

### Task 3: Airflow task 연결 (short_circuit + Slack 알림)

**Files:**
- Modify: `src/speed/bronze_validation.py` (Task 2에서 만든 파일에 추가)
- Modify: `dags/segment_time_pipeline.py`
- Test: `tests/speed/test_bronze_validation.py` (Task 2 파일에 추가)

**Interfaces:**
- Consumes: Task 2의 `CriticalValidationError`, `validate_bronze_file(bronze_path) -> list[dict]`; `src/common/alerts.py`의 `notify_slack_message(text: str) -> None`
- Produces: `_validate_and_decide(bronze_path: str) -> bool`(테스트 가능한 순수 로직), `validate_bronze`(`@task.short_circuit`로 감싼 얇은 wrapper, DAG가 import해서 씀)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/speed/test_bronze_validation.py`에 추가:

```python
from unittest.mock import patch


def test_validate_and_decide_returns_true_when_bronze_path_empty():
    # collect_bronze()가 빈 문자열을 반환하는 경우(신규 데이터 없음) -
    # check_new_data가 이미 이전 단계에서 걸렀어야 하지만 방어적으로도
    # 통과시킨다.
    with patch.object(bronze_validation, "validate_bronze_file") as mock_validate:
        result = bronze_validation._validate_and_decide("")

    assert result is True
    mock_validate.assert_not_called()


def test_validate_and_decide_returns_false_and_alerts_on_critical_failure():
    with patch.object(
        bronze_validation, "validate_bronze_file",
        side_effect=CriticalValidationError("필수 컬럼 없음: ['speed']"),
    ), patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is False
    mock_notify.assert_called_once()
    assert "speed" in mock_notify.call_args.args[0]


def test_validate_and_decide_returns_true_and_alerts_on_log_only_failure():
    failed = [{
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "speed"},
        "result": {"unexpected_count": 3},
    }]
    with patch.object(bronze_validation, "validate_bronze_file", return_value=failed), \
         patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is True
    mock_notify.assert_called_once()
    assert "1건" in mock_notify.call_args.args[0]


def test_validate_and_decide_returns_true_without_alert_when_all_pass():
    with patch.object(bronze_validation, "validate_bronze_file", return_value=[]), \
         patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is True
    mock_notify.assert_not_called()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/speed/test_bronze_validation.py -v -k validate_and_decide`
Expected: FAIL — `AttributeError: module 'src.speed.bronze_validation' has no attribute '_validate_and_decide'`

- [ ] **Step 3: 최소 구현 작성**

`src/speed/bronze_validation.py` 맨 아래에 추가:

```python
from airflow.decorators import task

from src.common.alerts import notify_slack_message


def _validate_and_decide(bronze_path: str) -> bool:
    """실제 결정 로직 - critical 실패시 False+Slack, log_only 실패시
    True+Slack+로그, 전부 통과(또는 검증할 파일 자체가 없음)시 True.

    @task.short_circuit는 Airflow TaskFlow 데코레이터라 직접 단위 테스트가
    번거로우므로, 분기 로직은 이 plain 함수에 두고 validate_bronze는 얇은
    wrapper로만 둔다.
    """

    if not bronze_path:
        return True

    try:
        failed_log_only = validate_bronze_file(bronze_path)
    except CriticalValidationError as error:
        logger.error(f"speed Bronze critical 검증 실패: {error}")
        notify_slack_message(
            f":red_circle: speed Bronze critical 검증 실패 - 이번 사이클 스킵\n{error}"
        )
        return False

    if failed_log_only:
        for check in failed_log_only:
            logger.warning(
                "speed Bronze 검증 실패(로그만): %s %s -> %s",
                check["expectation_type"], check["kwargs"], check["result"],
            )
        notify_slack_message(
            f":warning: speed Bronze log_only 검증 실패 {len(failed_log_only)}건 "
            f"(처리는 계속됨)"
        )

    return True


@task.short_circuit
def validate_bronze(bronze_path: str) -> bool:
    """collect_bronze() 직후, EMR 제출 전에 도는 게이트.

    False를 반환하면 Airflow가 이 task를 upstream으로 잡은 downstream
    task를 전부 스킵한다(on_failure_callback은 안 걸림 - 실패가 아니라
    정상 스킵이라서 - 그래서 위 _validate_and_decide 안에서 critical
    실패 시 Slack을 직접 호출한다).
    """

    return _validate_and_decide(bronze_path)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/speed/test_bronze_validation.py -v`
Expected: PASS (11 tests — Task 2의 7개 + 이번에 추가한 4개)

- [ ] **Step 5: DAG에 연결**

`dags/segment_time_pipeline.py`의 import 블록(파일 상단)을 수정한다:

```python
from src.speed.bronze import collect_speed_data, has_new_speed_data
from src.speed.bronze_validation import validate_bronze
```

(기존 `from src.speed.bronze import collect_speed_data, has_new_speed_data` 줄 바로 아래에 새 import 줄 추가)

`segment_time_pipeline()` 함수 본문 끝부분(현재 아래와 같이 되어 있는 곳):

```python
    new_data = check_new_data()
    bronze_path = collect_bronze()
    bronze_path.set_upstream(new_data)

    dim_segment_ready = check_dim_segment_exists()

    submit_result = submit_nav_time_job(bronze_path)
    submit_result.set_upstream(dim_segment_ready)
```

다음으로 교체한다:

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

- [ ] **Step 6: DAG가 정상적으로 import되는지 확인**

Run: `python3 -c "from dags.segment_time_pipeline import segment_time_pipeline"`
Expected: 에러 없이 조용히 끝남(예외 없음)

- [ ] **Step 7: 전체 테스트 스위트 확인**

Run: `pytest tests/speed/ -v`
Expected: PASS (전체)

- [ ] **Step 8: 커밋**

```bash
git add src/speed/bronze_validation.py tests/speed/test_bronze_validation.py dags/segment_time_pipeline.py
git commit -m "feat: segment_time_pipeline에 speed Bronze GX 검증 task 연결"
```
