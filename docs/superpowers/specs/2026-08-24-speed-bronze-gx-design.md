# speed Bronze GX 검증 Design

**Goal:** `segment_time_pipeline`이 NYC DOT 실시간 속도 API에서 받아온 Bronze 데이터를 EMR에 넘기기 전에, Great Expectations로 스키마/값 이상을 걸러낸다. 목표는 두 가지 실패 모드를 구분해서 잡는 것 — (1) 컬럼이 아예 사라지는 스키마 변경, (2) 컬럼은 남아있지만 값이 비정상(null 급증, 범위 이탈)인 조용한 데이터 품질 저하.

**Architecture:** TLC Bronze 검증(`src/tlc/bronze_validation.py` + `src/tlc/expectations.py`)과 동일한 critical/log_only 2단 구조를 따르되, speed 전용 모듈로 새로 만든다. TLC는 파일이 여러 개(taxi_type × 월)라 실패한 파일만 제외하고 계속 진행하지만, speed는 30분 주기당 Bronze 파일이 하나뿐이라 "파일 제외" 개념이 없다 — critical 실패는 이번 파이프라인 사이클 전체를 스킵하는 것으로 대응한다.

**Tech Stack:** 기존 `src/common/gx.py`의 `validate_pandas_dataframe`(pandas 기반 — Bronze 파일이 30분치라 작아서 Spark 세션 불필요), Airflow `@task.short_circuit`, 기존 `src/common/alerts.py`의 Slack 알림 함수.

## Global Constraints

- 이 설계 범위는 **speed Bronze만**이다. Silver1(`clean_speed_silver1`, EMR job 내부)은 이번 범위에서 제외 — 후속 과제.
- 새 검증 task는 `collect_bronze()` 직후, `submit_nav_time_job()`(EMR 제출) 이전에 실행한다.
- critical 실패 → 이번 사이클만 스킵(EMR 제출 안 함), 다음 30분 사이클에서 자동 재시도, Slack 알림 발송.
- log_only 실패 → 배치 처리는 그대로 진행(EMR 제출 계속), Slack 알림 발송 + 로그. (TLC의 log_only는 Slack을 안 보내고 로그만 남기는데, speed는 의도적으로 다르게 간다 — 아래 "TLC 패턴과의 차이" 참고)
- 검증 대상 DataFrame은 실제 API 응답 행과 synthetic 보강 행이 섞여 있다(`collect_speed_data()`가 이미 합쳐서 저장) — 두 종류를 구분하는 로직은 필요 없다. `src/speed/synthetic.py`의 `SPEED_COLUMNS`가 실제 API 응답과 컬럼 13개가 완전히 동일하고, 값도 실제스럽게 채워진다(예: `status="0"`, `owner="NYC-DOT"`).

## TLC 패턴과의 차이

| | TLC | speed |
|---|---|---|
| Bronze 파일 단위 | taxi_type × 월, 여러 개 | 30분 주기당 1개 |
| critical 실패 시 | 그 파일만 청크에서 제외, 나머지 계속 | 이번 사이클 전체 스킵 |
| log_only 실패 시 | 로그만 (Slack 없음) | 로그 + Slack 알림 |
| 실행 위치 | Airflow task, Spark 세션 | Airflow task, pandas (파일이 작음) |

log_only에 Slack을 추가한 이유: speed는 컬럼명이 안 바뀌고 값만 조용히 비어가는(null 급증) 스키마 드리프트 시나리오가 실제로 가능한데(브레인스토밍 중 확인), 이건 critical 검증(컬럼 존재 여부)으로는 못 잡고 log_only로만 잡힌다. 로그에만 남기면 사람이 놓치기 쉬워서 바로 알림이 가도록 한다.

## 검증 항목

### critical (컬럼이 실제로 사라졌는지)

다운스트림 `clean_speed_silver1()`이 실제로 참조하는 컬럼만 대상으로 한다:

```python
gx.expectations.ExpectColumnToExist(column="speed")
gx.expectations.ExpectColumnToExist(column="link_points")
gx.expectations.ExpectColumnToExist(column="data_as_of")
gx.expectations.ExpectColumnToExist(column="link_id")
```

### log_only (컬럼은 있지만 값이 이상한지)

```python
gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None)
gx.expectations.ExpectColumnValuesToNotBeNull(column="speed", mostly=0.90)
gx.expectations.ExpectColumnValuesToNotBeNull(column="link_points", mostly=0.90)
gx.expectations.ExpectColumnValuesToNotBeNull(column="data_as_of", mostly=0.90)
gx.expectations.ExpectColumnValuesToNotBeNull(column="link_id", mostly=0.90)
gx.expectations.ExpectColumnValuesToBeBetween(column="speed", min_value=0, max_value=150)
gx.expectations.ExpectColumnValuesToBeBetween(
    column="data_as_of", min_value="2017-01-01", max_value=<오늘+1일>,
)
```

null 체크(`mostly=0.90`)는 컬럼별로 독립 적용된다 — 예를 들어 `data_as_of`만 10% 넘게 비어도 그 컬럼만 걸리고 나머지는 안 걸린다. 임계치 10%는 개별 센서의 산발적 결측(노이즈)은 넘기고, 스키마 드리프트처럼 값이 뭉텅이로 비는 경우만 잡기 위한 값이다.

`data_as_of` 날짜 범위 검증은 브레인스토밍 중 실제로 라이브 API에서 `1930-12-09` 같은 이상치를 발견해서 추가한 항목이다(`min_value="2017-01-01"`은 이 데이터셋 생성일자 `createdAt=2017-04-17` 기준). `max_value`는 검증 실행 시점 기준 오늘+1일로 동적으로 계산한다(먼 미래 타임스탬프도 같은 종류의 이상치이므로).

**주의(실제로 재현 확인한 버그) — `speed`/`data_as_of`는 Bronze에 문자열로 저장된다.** Socrata API가 모든 필드를 문자열로 주기 때문에(`"speed":"29.82"`), `collect_speed_data()`가 만드는 Bronze parquet은 `speed`/`data_as_of` 컬럼이 전부 `object`(string) dtype이다(실제 파일로 확인함). `ExpectColumnValuesToBeBetween`을 문자열 컬럼에 그대로 돌리면 GX가 `TypeError: Column values, min_value, and max_value must either be None or of the same type`를 내부적으로 삼켜서 `success=False, result={}`만 남기고 진짜 원인은 `exception_info`에만 남는다(실제로 재현해서 확인함 — `src/common/gx.py`의 docstring이 경고하는 바로 그 실패 모드). 그래서 검증 직전에 **검증용 복사본**에서만 캐스팅한다:

```python
validation_df = df.copy()
validation_df["speed"] = pd.to_numeric(validation_df["speed"], errors="coerce")
validation_df["data_as_of"] = pd.to_datetime(validation_df["data_as_of"], errors="coerce")
```

`errors="coerce"`로 파싱 안 되는 값은 예외 대신 null(NaN/NaT)로 만든다 — 그러면 그 값은 not-null 체크(위 2~5번)에서 자연스럽게 잡힌다. 원본 Bronze parquet 파일 자체는 이 캐스팅과 무관하게 그대로 문자열로 저장된다(Bronze 원칙 — 변환 없음).

## 실행 위치 — DAG 변경

`dags/segment_time_pipeline.py`에 새 task 추가:

```python
@task
def validate_bronze(bronze_path: str) -> str:
    """critical 실패 시 CriticalValidationError를 던져 short_circuit이 이번
    사이클을 스킵하게 한다. 통과하면 bronze_path를 그대로 다음 task에 전달."""
    ...

@task.short_circuit
def bronze_is_valid(...) -> bool:
    ...
```

정확한 task 분리(단일 task에서 예외+short_circuit을 어떻게 조합할지)는 구현 계획에서 확정한다 — `check_dim_segment_exists`가 이미 이 DAG 안에 있는 `@task.short_circuit` 패턴 참고.

**주의(구현 시 놓치기 쉬운 부분):** DAG에 이미 걸려있는 `on_failure_callback=notify_slack_failure`(default_args)는 **task가 실패(exception)할 때만** 발동한다. short_circuit으로 "건너뛰기"는 Airflow 입장에서 실패가 아니라 정상 스킵이라 이 콜백이 안 걸린다. 그래서 critical 실패 시 Slack 알림은 `on_failure_callback`에 의존하지 말고, 검증 task 안에서 `notify_slack_message`(또는 동급 함수)를 **직접 호출**해야 한다.

## 새 파일

- `src/speed/expectations.py` — critical/log_only 검증 목록 (TLC의 `src/tlc/expectations.py`와 동일한 역할, taxi_type별 분기가 없어서 더 단순함)
- `src/speed/bronze_validation.py` — `validate_bronze_file()` 등, TLC의 `src/tlc/bronze_validation.py`와 동일한 역할

## 알려진 리스크 / 후속 과제 (이 설계 범위 밖)

- Silver1(`clean_speed_silver1`) 검증은 이번 범위에서 제외했다 — 나중에 필요하면 EMR job(`nav_time_job.py`) 안에서 Spark DataFrame으로 변환한 직후 `validate_spark_dataframe`을 붙이는 방식이 유력하다(이미 Spark로 돌고 있어서 새 세션 불필요).
- `src/common/gx.py`의 검증 실행이 현재 expectation별 개별 `batch.validate()` 루프다(Suite 배치 최적화가 적용된 적 없음 — 세션 초반에 확인된 사실, 메모리에 남아있던 "완료" 기록은 틀렸었다). speed는 파일이 작아서 이 성능 이슈에 해당하지 않지만, 다른 도메인에 GX를 넓힐 때는 참고할 것.
