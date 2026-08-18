# TLC Bronze Great Expectations 데이터 품질 검증 설계

## 배경

현재 데이터 품질 검증은 `src/common/validator.py`의 `validate_download()`가 전부다. 이 함수는
다운로드된 파일이 존재하는지, 크기가 0이 아닌지만 확인한다 — 파일을 열어보지 않는 IO/전송
레벨 체크다.

TLC 파이프라인은 외부 소스(NYC TLC)에서 매달 새 parquet 파일을 받아오는데, 원본 데이터는
컬럼이 추가/삭제되거나 이름이 바뀔 수 있고, 특정 컬럼의 null 비율이 비정상적으로 높아지는
등 콘텐츠 레벨 문제가 생길 수 있다. 이런 문제는 IO 체크로는 잡히지 않고 그대로
Silver → Gold → Traffic Score까지 흘러간다.

실제로 `src/tlc/transform.py`의 `rename_columns()`는 이미 "필수 원본 컬럼이 없으면
`ValueError`" 체크를 하고 있지만, 이 체크는 Silver 변환 시점(청크당 Spark 세션을 이미 연
뒤)에야 실행된다. 이 문제를 Bronze 적재 직후로 앞당기고, 컬럼 존재 여부 외의 값 수준
검증(범위, null 비율 등)까지 포함해 체계적으로 관리하기 위해 Great Expectations(GX)를
도입한다.

## 목표

TLC Bronze 파일이 Silver로 넘어가기 전에 스키마/값 수준 문제를 자동으로 잡아내고, 심각도에
따라 "그 파일만 제외" 또는 "로그만 기록"으로 대응을 자동화한다. 동시에 향후 다른
도메인(construction, event 등)이 같은 인프라를 재사용할 수 있는 공통 구조를 만든다.

## 범위

**포함**

- `src/common/`에 GX 실행 러너(공통 인프라) 추가
- TLC Bronze 레이어에 대한 Expectation Suite 정의 및 검증 Task 추가
- `tlc_pipeline` DAG에 검증 Task 삽입
- 검증 실패 시 심각도별 대응(파일 제외 + Slack 알림 / 로그만) 구현
- GX·PySpark 4.0.4 호환성 확인 (완료, 아래 "기술 검증" 참고)

**제외 (후속 작업)**

- Silver/Gold 레이어에 대한 GX 검증. 이번 설계는 Bronze만 다룬다.
- TLC 이외 도메인(construction, event, road_closures 등)의 Expectation Suite 작성. 공통
  러너는 재사용 가능하게 만들지만, 다른 도메인 suite는 각 담당자가 이 설계와 동일한 패턴으로
  별도 작성한다.
- 실패한 파일을 별도 quarantine 폴더로 물리적으로 옮기는 것. 이번 범위에서는 "Silver로 안
  넘긴다 + 로그/알림"까지만 하고, Bronze 파일 자체는 그대로 둔다.
- GX Data Docs(HTML 검증 리포트) 활성화. 필요해지면 후속으로 추가한다.
- null 비율 등 임계값 기반의 세분화된 심각도 체계. 이번 설계는 "컬럼 존재 여부"만 critical로
  다루고, 그 외 값 수준 이슈는 전부 로그로 통일한다 (아래 "검증 항목과 심각도" 참고).

## 기술 검증 (완료)

GX가 이 프로젝트의 PySpark 4.0.4(2025년 출시된 최신 메이저 버전)와 실제로 호환되는지는 공식
문서에 명시가 없어 직접 스파이크 테스트로 확인했다.

- 격리된 venv에 Python 3.11.15 + `pyspark==4.0.4`(프로젝트와 동일) + `great_expectations==1.20.0`
  설치
- Java 17(`Dockerfile`과 동일 조건)에서 `SparkSession` 생성, 3행짜리 테스트 DataFrame(1개
  null 포함) 생성
- GX 1.x Fluent API(`context.data_sources.add_spark()` → `add_dataframe_asset()` →
  `add_batch_definition_whole_dataframe()` → `batch.validate(expectation)`)로
  `ExpectColumnValuesToNotBeNull` 실행
- 결과: `element_count: 3, unexpected_count: 1, unexpected_percent: 33.3%, success: false`,
  `exception_info.raised_exception: false` — 예외 없이 null 1건을 정확히 검출

**결론**: GX 1.20.0 이상 + PySpark 4.0.4 조합은 `SparkDFExecutionEngine` 기준으로 정상 동작한다.
`requirements.txt`에 `great_expectations>=1.20.0` 추가가 필요하다.

## 아키텍처: 공통 vs 도메인

기존 `src/common/`의 관례(예: `spark.py`는 세션 생성이라는 공통 인프라를 제공하고, 실제 변환
로직인 `transform()`은 `src/tlc/`에 있음)를 그대로 따른다.

```
src/common/
  gx.py                  # GX 공통 러너 (신규)
  alerts.py              # Slack 알림 (기존 + 범용 메시지 함수 추가)
src/tlc/
  expectations.py        # TLC Bronze Expectation Suite 정의 (신규)
  bronze_validation.py   # Bronze 검증 Airflow Task (신규)
dags/
  tlc_pipeline.py        # 기존 DAG에 Task 삽입
```

- **`src/common/gx.py`**: GX Context 생성, "Spark DataFrame + ExpectationSuite를 받아 검증을
  실행하고 결과를 반환"하는 함수까지만 책임진다. 검증 실패 시 어떻게 반응할지(제외/로그/알림)는
  호출하는 도메인 Task(`bronze_validation.py`)가 결정한다 — 실행 로직과 반응 로직을 분리해서,
  나중에 다른 도메인이 같은 러너를 가져다 쓰되 자기 도메인에 맞는 반응 정책을 따로 정의할 수
  있게 한다.
- **`src/tlc/expectations.py`**: taxi_type(yellow/green/fhv/fhvhv)별로 원본 컬럼 구성이 달라
    (`transform.py`의 `COLUMN_MAPPING` 참고), taxi_type별로 별도 Suite를 정의한다.
- **`validator.py`와의 관계**: 대체하지 않는다. `validate_download()`는 다운로드 직후 파일
  존재/크기만 보는 가벼운 1차 게이트로 그대로 둔다. GX는 Bronze 적재 후 실제 데이터 내용을
  검사하는 2차 게이트로 추가된다.

## 데이터 흐름

```
download_file.expand()
  → validate_download.expand()        [기존, IO 체크]
  → store_bronze.expand()             [기존]
  → chunk_bronze_files()              [기존, taxi_type별 그룹화]
  → validate_bronze_quality.expand()  [신규]
  → build_silver.expand()             [기존, "통과한 파일만" 전달받음]
```

`validate_bronze_quality`는 `chunk_bronze_files`가 만든 taxi_type별 청크를 그대로 입력받고,
같은 taxi_type의 통과한 파일 목록만 다음 단계로 넘긴다. `build_silver`는 입력 형태(청크 안
파일 목록)가 그대로 유지되므로 별도 수정이 필요 없다 — 다만 청크 안 파일 수가 검증 전보다
줄어들 수 있다는 점만 다르다. 한 taxi_type의 모든 파일이 제외되어 빈 목록이 되는 경우에도
`build_silver`는 빈 목록을 그대로 순회(0회 반복)하고 빈 결과를 반환하므로 별도 처리가
필요 없다.

## 실행 전략: 청크 단위 세션 재사용 + 파일별 개별 판정

**문제**: `build_silver`는 taxi_type당 Spark 세션 하나를 열어 파일들을 순차 처리하는데, 파일
전체를 하나의 `try/except`로 묶어서 처리한다. 이 구조를 그대로 검증에 적용하면, 청크 안 파일
하나가 검증에 실패했을 때 예외가 루프 밖으로 전파되어 **같은 청크의 다른 정상 파일들까지 전부
막히는** 문제가 생긴다 (taxi_type이 같다는 이유만으로 묶여서 함께 실패 처리됨).

**해결**: `validate_bronze_quality`는 청크 단위로 Spark 세션 하나를 재사용하되(효율성 유지),
루프 안에서 파일마다 개별적으로 검증하고 판정한다(격리성 확보) — 한 파일의 검증 실패가 예외로
루프 밖까지 전파되지 않고, 그 파일만 결과 목록에서 제외된 채 루프가 계속 진행된다.

```
taxi_type 청크 하나 (Spark 세션 1개)
  for 파일 in 청크:
      try: 검증 실행 → 통과/실패 판정
      except: 이 파일만 실패 처리 (제외), 루프는 계속
  → 통과한 파일 목록만 반환
```

Task 자체가 죽는 경우(Spark 세션 기동 실패 등 검증 로직과 무관한 진짜 장애)는 이 개별 처리
대상이 아니며, 그대로 예외가 전파되어 기존 `on_failure_callback`(`notify_slack_failure`) 경로를
탄다 — 이 부분은 변경하지 않는다.

## 검증 항목과 심각도 (TLC Bronze, taxi_type별)

| 검증 항목 | 대상 | 심각도 | 실패 시 대응 |
|---|---|---|---|
| dropoff_datetime, dropoff_location_id의 **원본 컬럼 자체가 존재하지 않음** | 전체 taxi_type | **Critical** | 해당 파일 제외 + Slack 즉시 알림 |
| row count > 0 | 전체 taxi_type | Log-only | 로그 기록, 파일은 계속 진행 |
| pickup_datetime, pickup_location_id 등 나머지 필수 원본 컬럼 존재 여부 (taxi_type별 `COLUMN_MAPPING` 기준, fhv/fhvhv는 passenger_count·trip_distance 제외) | taxi_type별 상이 | Log-only | 로그 기록, 파일은 계속 진행 |
| pickup/dropoff datetime, location ID 등 **컬럼은 있지만 일부 행의 값이 null** | 전체 taxi_type | Log-only | 로그 기록 (기존 `check_null()`과 동일 정책 — 결측치가 있어도 삭제하지 않음) |
| location ID가 유효 범위(1~263) 안에 있는지 | 전체 taxi_type | Log-only | 로그 기록 |
| passenger_count, trip_distance(또는 trip_miles)가 음수가 아닌지 | 값이 존재하는 taxi_type만 | Log-only | 로그 기록 |

**심각도 판단 기준**: "컬럼 자체가 원본에 없다"는 것은 TLC가 데이터 포맷을 통째로 바꿨다는
뜻이라 드물고, 그 파일 전체가 해당 정보를 원천적으로 복구할 수 없는 상태다. 반면 "컬럼은
있는데 일부 행만 null"인 경우는 TLC 실제 데이터에서 흔히 발생하는 정상 범주의 잡음이라
`check_null()`이 이미 로그만 남기는 정책을 쓰고 있고, 이번 설계도 그 정책을 그대로 따른다.

dropoff_datetime/dropoff_location_id를 critical로 지정한 이유는 이 두 컬럼이 Silver의 6개
컬럼 중 traffic score 분석(세그먼트별 하차 위치·시각 집계)에 직접 쓰이는 핵심 값이기 때문이다.

## 실패 처리 상세

- **Critical (컬럼 없음)**: 검증 실행 직후 그 자리에서 Slack으로 알림을 보내야 하는데, 이건
  Airflow Task 자체의 최종 실패가 아니라 "정상 동작 중 특정 파일을 걸러낸 것"이라 기존
  `notify_slack_failure`(Task 최종 실패 시 `on_failure_callback`으로만 호출됨)가 자동으로
  발동하지 않는다. 따라서 `src/common/alerts.py`에 Airflow context에 의존하지 않는 범용 메시지
  전송 함수(가칭 `notify_slack_message(text: str)`)를 추가하고, `bronze_validation.py`가
  critical 실패를 감지했을 때 이 함수를 직접 호출한다. 기존 `notify_slack_failure`와 마찬가지로
  알림 전송 자체가 실패해도 예외를 밖으로 던지지 않는다(로그만 남김).
- **Log-only**: `src/common/logger.py`의 로거로 어떤 expectation이 왜 실패했는지(파일명,
  taxi_type, 컬럼, 실패 건수/비율)를 기록한다. Slack 알림은 보내지 않는다.
- **Task 레벨 장애**: 검증 로직과 무관한 예외(Spark 세션 기동 실패 등)는 그대로 전파해 Task를
  실패시키고, 기존 DAG의 `on_failure_callback=notify_slack_failure` 경로를 그대로 사용한다.

## 구현 위치 제안

```
src/common/gx.py                # GX Context 생성 + 검증 실행 러너
src/common/alerts.py            # notify_slack_message() 추가 (기존 파일 확장)
src/tlc/expectations.py         # taxi_type별 ExpectationSuite 정의
src/tlc/bronze_validation.py    # validate_bronze_quality Task (청크 순회 + 개별 판정 + 반응)
dags/tlc_pipeline.py            # store_bronze와 chunk_bronze_files 사이에 Task 삽입
requirements.txt                # great_expectations>=1.20.0 추가
```

구체적인 함수 시그니처와 내부 구현 순서는 다음 단계(구현 계획)에서 정한다.

## 향후 확장 (이번 설계 범위 밖)

- Silver/Gold 레이어에도 GX 검증 추가 (예: Silver의 `check_null()`을 GX 기반으로 대체하거나
  보완).
- 다른 도메인(construction, event, road_closures 등)이 `src/common/gx.py` 러너를 재사용해
  각자의 Expectation Suite 작성.
- 실패한 파일을 별도 quarantine 폴더로 이동해 원인 분석을 쉽게 하는 것.
- GX Data Docs를 활성화해 검증 이력을 HTML 리포트로 축적.
- null 비율 등 값 수준 이슈에도 임계값 기반 심각도(예: 5% 초과 시 critical)를 도입.
