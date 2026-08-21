# Segment Metrics Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** type1(시간)/type2(길이) 파이프라인과 서빙 API가 공통으로 의존하는 기반(DynamoDB 접근 모듈, EMR Serverless 제출 헬퍼, 테이블 생성/기본값 시딩 스크립트, 로컬 개발 환경)을 만든다.

**Architecture:** `src/common/dynamodb.py`는 boto3 `resource("dynamodb")`를 감싸는 얇은 헬퍼(테이블 핸들 조회, 100개 단위 청크 `batch_get_item`, `put_item`)만 제공한다 — fallback 체인 같은 비즈니스 로직은 포함하지 않는다(서빙 API 쪽 플랜에서 구현). `src/common/emr_serverless.py`는 Spark job을 EMR Serverless에 제출/대기하는 헬퍼다. 로컬 개발/테스트는 `amazon/dynamodb-local` 컨테이너 + `moto`로 AWS 자격증명 없이 동작한다.

**Tech Stack:** boto3, moto[dynamodb], Docker(dynamodb-local), pytest

## Global Constraints

- 설계 문서: `docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md` (모든 태스크가 이 문서와 충돌하면 설계 문서가 우선)
- DynamoDB 테이블은 타입별로 완전히 분리한다: `SegmentMetricsType1`(시간), `SegmentMetricsType2`(길이) — 접두사 컨벤션 아님, 물리적으로 다른 테이블
- 파티션 키(PK)는 `segment_id`(문자열), 정렬 키(SK)는 `sk`(문자열)로 통일
- GLOBAL 파티션(`segment_id="GLOBAL"`)의 기본값 항목은 파이프라인이 아니라 이 플랜의 시딩 스크립트가 배포 시점에 수동으로 넣는다 — 파이프라인 코드는 이 항목을 절대 쓰지 않는다
- 기존 관례를 따른다: 모든 공용 모듈은 `src/common/`, 로거는 `get_logger(__name__, log_to_file=True, log_file_stem="...")` 패턴 사용, 예외는 `logger.exception`/`logger.error` 후 `raise`

---

## File Structure

- Modify: `src/common/config.py` — DynamoDB 테이블명, GLOBAL 파티션 키/기본값 SK, 버킷 크기, 롤링 윈도우, EMR 상수 추가
- Create: `src/common/dynamodb.py` — DynamoDB 리소스/테이블 핸들, 청크 `batch_get_item`, `put_item` 헬퍼
- Create: `src/common/emr_serverless.py` — EMR Serverless Spark job 제출/대기 헬퍼
- Modify: `.env.example` — EMR/DynamoDB 관련 변수 추가
- Modify: `docker-compose.yml` — `dynamodb-local` 서비스 추가
- Modify: `requirements.txt` — `moto[dynamodb]` 추가
- Create: `scripts/create_dynamodb_tables.py` — 두 테이블 생성(idempotent)
- Create: `scripts/seed_dynamodb_defaults.py` — GLOBAL 기본값 시딩
- Create: `tests/common/test_dynamodb.py`
- Create: `tests/common/test_emr_serverless.py`

---

### Task 1: config.py에 DynamoDB/EMR 상수 추가

**Files:**
- Modify: `src/common/config.py` (파일 끝에 새 섹션 추가)

**Interfaces:**
- Produces: `DYNAMODB_TABLE_TYPE1: str`, `DYNAMODB_TABLE_TYPE2: str`, `DYNAMODB_ENDPOINT_URL: str | None`, `GLOBAL_PARTITION_KEY: str = "GLOBAL"`, `DEFAULT_SORT_KEY: str = "DEFAULT"`, `AVG_SORT_KEY: str = "AVG"`, `LENGTH_SORT_KEY: str = "LENGTH"`, `BUCKET_MINUTES: int = 30`, `ROLLING_WINDOW_DAYS: int = 14`, `EMR_APPLICATION_ID: str | None`, `EMR_JOB_ROLE_ARN: str | None`, `EMR_JOBS_DIR`(S3Path 또는 로컬 Path, `BRONZE_DIR`와 동일한 `APP_ENV` 분기 패턴)

- [ ] **Step 1: config.py 끝에 상수 추가**

`src/common/config.py` 파일 맨 끝(245번째 줄 이후)에 추가:

```python
# ==========================
# 세그먼트 지표 API — DynamoDB 서빙 저장소 설정
# ==========================
#
# 타입별로 완전히 분리된 테이블을 쓴다(팀원이 타입별로 독립 개발하기 때문 —
# 접두사 컨벤션이 아니라 물리적으로 다른 테이블). 자세한 설계 근거는
# docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md 6절 참고.

DYNAMODB_TABLE_TYPE1 = os.getenv("DYNAMODB_TABLE_TYPE1", "SegmentMetricsType1")
DYNAMODB_TABLE_TYPE2 = os.getenv("DYNAMODB_TABLE_TYPE2", "SegmentMetricsType2")

# APP_ENV=local이면 dynamodb-local 컨테이너를 가리킨다. aws(EC2)에서는 빈 값으로
# 둬서 boto3가 기본 리전 엔드포인트를 쓰게 한다(다른 AWS 자격증명 설정과 동일한
# 패턴 — 여기서 없다고 에러내지 않는다, 실제 클라이언트 생성 시점에서만 확인).
DYNAMODB_ENDPOINT_URL = (
    "http://dynamodb-local:8000" if APP_ENV == "local" else None
)

# Fallback 체인(설계 문서 7절)에서 쓰는 예약 키.
# GLOBAL_PARTITION_KEY: 실제 segment_id가 아닌 예약된 PK — 배포 시점에 수동으로
#   심어두는 전역 기본값 전용 파티션.
GLOBAL_PARTITION_KEY = "GLOBAL"
DEFAULT_SORT_KEY = "DEFAULT"
AVG_SORT_KEY = "AVG"
LENGTH_SORT_KEY = "LENGTH"

# 하루를 30분 단위로 나눈 버킷 수(00:00~23:30 -> 48개). 버킷 키는 "HHMM" 문자열.
BUCKET_MINUTES = 30

# type1(시간) 버킷 값을 계산할 때 참고하는 최근 관측치 범위(일). 조정 가능한
# 파라미터라 상수로 뺐다 — 실측 후 조정.
ROLLING_WINDOW_DAYS = 14

# ==========================
# EMR Serverless (Spark job 실행) 설정
# ==========================
#
# Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
# 담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다
# (src/common/emr_serverless.py 참고).

EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")

if APP_ENV == "local":
    EMR_JOBS_DIR = PROJECT_ROOT / "data" / "emr-jobs"
else:
    EMR_JOBS_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/emr-jobs")
```

- [ ] **Step 2: import 확인**

`config.py` 상단에 이미 `import os`, `from pathlib import Path`, `from cloudpathlib import S3Path`가 있으므로 추가 import 불필요. 아래 명령으로 문법 오류 없이 로드되는지 확인한다.

Run: `python -c "from src.common import config; print(config.DYNAMODB_TABLE_TYPE1, config.EMR_JOBS_DIR)"`
Expected: `SegmentMetricsType1 /Users/.../DE-Project/data/emr-jobs` (APP_ENV가 .env에서 local이 아니면 S3Path가 출력됨 — 둘 다 에러 없이 출력되면 정상)

- [ ] **Step 3: Commit**

```bash
git add src/common/config.py
git commit -m "feat: DynamoDB/EMR Serverless 관련 설정 상수 추가"
```

---

### Task 2: requirements.txt에 moto 추가

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: moto 추가**

`requirements.txt`의 `boto3` 줄 바로 아래에 추가:

```
boto3
cloudpathlib[s3]
s3fs

# DynamoDB 로컬 모킹 테스트용(src/common/dynamodb.py, src/common/emr_serverless.py 테스트).
moto[dynamodb]
```

- [ ] **Step 2: 설치 확인**

Run: `pip install -r requirements.txt`
Expected: `moto` 및 하위 의존성이 정상 설치됨 (에러 없이 종료)

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: DynamoDB 모킹 테스트용 moto 의존성 추가"
```

---

### Task 3: `src/common/dynamodb.py` — DynamoDB 접근 헬퍼

**Files:**
- Create: `src/common/dynamodb.py`
- Test: `tests/common/test_dynamodb.py`

**Interfaces:**
- Consumes: `config.DYNAMODB_ENDPOINT_URL`, `config.AWS_REGION`
- Produces: `get_dynamodb_resource() -> boto3.resources.base.ServiceResource`, `get_table(table_name: str)`, `batch_get_items(table_name: str, keys: list[dict]) -> dict[tuple[str, str], dict]`(키는 `{"segment_id": ..., "sk": ...}`, 반환은 `(segment_id, sk) -> item` 맵, 못 찾은 키는 결과에 없음), `put_item(table_name: str, item: dict) -> None`, `batch_write_items(table_name: str, items: list[dict]) -> None`(파이프라인 Gold2 단계가 대량 upsert할 때 씀 — boto3 `Table.batch_writer()`로 25개 단위 자동 배치)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/common/test_dynamodb.py`:

```python
import boto3
import pytest
from moto import mock_aws

from src.common import dynamodb


TABLE_NAME = "TestSegmentMetrics"


def _create_test_table(region="us-east-1"):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "segment_id", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "segment_id", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@mock_aws
def test_put_item_then_batch_get_returns_it():
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "1", "sk": "1200", "value": 30})

    result = dynamodb.batch_get_items(
        TABLE_NAME, [{"segment_id": "1", "sk": "1200"}]
    )

    assert result[("1", "1200")]["value"] == 30


@mock_aws
def test_batch_get_missing_key_is_absent_from_result():
    _create_test_table()

    dynamodb.put_item(TABLE_NAME, {"segment_id": "1", "sk": "1200", "value": 30})

    result = dynamodb.batch_get_items(
        TABLE_NAME,
        [{"segment_id": "1", "sk": "1200"}, {"segment_id": "999", "sk": "1200"}],
    )

    assert ("1", "1200") in result
    assert ("999", "1200") not in result


@mock_aws
def test_batch_get_chunks_over_100_keys():
    _create_test_table()

    for i in range(150):
        dynamodb.put_item(TABLE_NAME, {"segment_id": str(i), "sk": "1200", "value": i})

    keys = [{"segment_id": str(i), "sk": "1200"} for i in range(150)]
    result = dynamodb.batch_get_items(TABLE_NAME, keys)

    assert len(result) == 150
    assert result[("149", "1200")]["value"] == 149


@mock_aws
def test_batch_get_empty_keys_returns_empty_dict():
    _create_test_table()

    result = dynamodb.batch_get_items(TABLE_NAME, [])

    assert result == {}


@mock_aws
def test_batch_write_items_then_get_all():
    _create_test_table()

    items = [{"segment_id": str(i), "sk": "LENGTH", "value": i * 10} for i in range(30)]
    dynamodb.batch_write_items(TABLE_NAME, items)

    result = dynamodb.batch_get_items(
        TABLE_NAME, [{"segment_id": str(i), "sk": "LENGTH"} for i in range(30)]
    )

    assert len(result) == 30
    assert result[("29", "LENGTH")]["value"] == 290
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/common/test_dynamodb.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.dynamodb'`

- [ ] **Step 3: `src/common/dynamodb.py` 구현**

```python
"""
DynamoDB 공용 접근 헬퍼

boto3 저수준 배치 조회/저장만 감싼다. fallback 체인(정확 값 -> AVG ->
GLOBAL#DEFAULT -> 코드 상수) 같은 비즈니스 로직은 여기 두지 않는다 —
서빙 API(src/serving/nav_api.py)가 이 모듈의 batch_get_items()를 호출해서
"없는 키는 결과에 없다"는 사실 자체를 fallback 트리거로 쓴다.

배치 조회 시 키가 없는 경우와 DynamoDB 호출 자체가 실패(예외)하는 경우를
호출부가 구분해서 처리할 수 있도록, 이 모듈은 예외를 삼키지 않고 그대로
던진다 — 호출부(서빙 API)가 그 예외를 잡아서 fallback으로 넘어간다.
"""

from __future__ import annotations

import boto3

from src.common.config import AWS_REGION, DYNAMODB_ENDPOINT_URL
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="dynamodb")

# DynamoDB BatchGetItem 한 번에 요청 가능한 최대 키 개수(AWS 하드 리밋).
_BATCH_GET_MAX_KEYS = 100


def get_dynamodb_resource():
    """DynamoDB 리소스를 반환한다.

    APP_ENV=local이면 DYNAMODB_ENDPOINT_URL(dynamodb-local 컨테이너)을 쓰고,
    아니면 기본 AWS 엔드포인트를 쓴다.
    """
    kwargs = {"region_name": AWS_REGION}
    if DYNAMODB_ENDPOINT_URL:
        kwargs["endpoint_url"] = DYNAMODB_ENDPOINT_URL

    return boto3.resource("dynamodb", **kwargs)


def get_table(table_name: str):
    """테이블 핸들을 반환한다."""
    return get_dynamodb_resource().Table(table_name)


def _chunk(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def batch_get_items(table_name: str, keys: list[dict]) -> dict[tuple[str, str], dict]:
    """(segment_id, sk) 키 목록으로 여러 항목을 한 번에 조회한다.

    100개 초과분은 여러 BatchGetItem 요청으로 자동 청크 분할한다. 반환값은
    (segment_id, sk) -> item 딕셔너리이며, DynamoDB에 없는 키는 결과에서
    빠진다(호출부가 이걸로 fallback 여부를 판단한다).
    """
    if not keys:
        return {}

    resource = get_dynamodb_resource()
    result: dict[tuple[str, str], dict] = {}

    for chunk in _chunk(keys, _BATCH_GET_MAX_KEYS):
        request_keys = list(chunk)

        # DynamoDB가 처리량 제한 등으로 일부만 처리하고 나머지를
        # UnprocessedKeys로 돌려줄 수 있다 — 전부 처리될 때까지 재요청한다.
        while request_keys:
            response = resource.batch_get_item(
                RequestItems={table_name: {"Keys": request_keys}}
            )

            for item in response["Responses"].get(table_name, []):
                result[(item["segment_id"], item["sk"])] = item

            unprocessed = response.get("UnprocessedKeys", {})
            request_keys = unprocessed.get(table_name, {}).get("Keys", [])

            if request_keys:
                logger.warning(
                    "DynamoDB batch_get_item 미처리 키 재요청: table=%s count=%d",
                    table_name,
                    len(request_keys),
                )

    return result


def put_item(table_name: str, item: dict) -> None:
    """항목 하나를 저장(upsert)한다."""
    get_table(table_name).put_item(Item=item)


def batch_write_items(table_name: str, items: list[dict]) -> None:
    """여러 항목을 한 번에 저장(upsert)한다.

    파이프라인 Gold2 단계가 세그먼트 수천~수십만 건을 한 번에 upsert할 때
    쓴다. boto3 Table.batch_writer()가 내부적으로 25개 단위 BatchWriteItem
    요청과 처리량 제한 시 자동 재시도까지 처리한다.
    """
    if not items:
        return

    table = get_table(table_name)
    with table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/common/test_dynamodb.py -v`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add src/common/dynamodb.py tests/common/test_dynamodb.py
git commit -m "feat: DynamoDB 배치 조회/저장 공용 헬퍼 추가"
```

---

### Task 4: `src/common/emr_serverless.py` — EMR Serverless 제출 헬퍼

**Files:**
- Create: `src/common/emr_serverless.py`
- Test: `tests/common/test_emr_serverless.py`

**Interfaces:**
- Consumes: `config.AWS_REGION`, `config.EMR_APPLICATION_ID`, `config.EMR_JOB_ROLE_ARN`, `config.EMR_JOBS_DIR`, `config.PROJECT_ROOT`, `config.TMP_DIR`
- Produces: `run_spark_job(job_name: str, entry_point_script: Path, entry_point_args: list[str]) -> None` — 실패(FAILED/CANCELLED) 시 `RuntimeError` 발생

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/common/test_emr_serverless.py`:

```python
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.common import emr_serverless


@pytest.fixture
def tmp_script(tmp_path):
    script = tmp_path / "job.py"
    script.write_text("print('hello')")
    return script


def test_run_spark_job_success(tmp_script):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        emr_serverless.run_spark_job("test-job", tmp_script, ["--foo", "bar"])

    mock_client.start_job_run.assert_called_once()
    call_kwargs = mock_client.start_job_run.call_args.kwargs
    assert call_kwargs["jobDriver"]["sparkSubmit"]["entryPointArguments"] == ["--foo", "bar"]


def test_run_spark_job_raises_on_failure(tmp_script):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {
        "jobRun": {"state": "FAILED", "stateDetails": "boom"}
    }

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        with pytest.raises(RuntimeError, match="boom"):
            emr_serverless.run_spark_job("test-job", tmp_script, [])
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/common/test_emr_serverless.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.emr_serverless'`

- [ ] **Step 3: `src/common/emr_serverless.py` 구현**

```python
"""
EMR Serverless Spark 잡 제출/대기 헬퍼

Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다.
src/ 전체를 zip으로 묶어 --py-files로 넘겨서, 잡 스크립트가 src.tlc.* 등
기존 순수 변환 함수를 그대로 import해서 쓸 수 있게 한다 — 변환 로직을
spark_jobs 쪽에 복제하지 않기 위함이다.
"""

from __future__ import annotations

import json
import time
import zipfile
from pathlib import Path

import boto3
from cloudpathlib import S3Path

from src.common.config import (
    AWS_REGION,
    EMR_APPLICATION_ID,
    EMR_JOB_ROLE_ARN,
    EMR_JOBS_DIR,
    PROJECT_ROOT,
    TMP_DIR,
)
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="emr_serverless")

# EMR Serverless 배치 잡은 몇 분 단위로 걸리는 게 보통이라, 상태를 너무
# 자주 조회해서 API를 낭비할 필요가 없다.
_POLL_INTERVAL_SECONDS = 15
_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}


def _upload_src_bundle() -> str:
    """src/ 디렉터리를 zip으로 묶어 EMR_JOBS_DIR에 올리고 경로를 반환한다.

    잡마다 매번 새로 올린다 — src/가 몇백 KB 수준이라 비용/시간 부담이
    거의 없고, 코드가 바뀐 채로 캐시된 옛 zip을 잘못 쓰는 사고를 막는다.
    """
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = TMP_DIR / "emr_src_bundle.zip"

    src_dir = PROJECT_ROOT / "src"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in src_dir.rglob("*.py"):
            zf.write(path, arcname=path.relative_to(PROJECT_ROOT))

    dest = EMR_JOBS_DIR / "bundles" / "src.zip"

    if isinstance(dest, Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(zip_path.read_bytes())
    else:
        dest.upload_from(zip_path)

    zip_path.unlink()

    return str(dest)


def _upload_script(local_path: Path, job_name: str) -> str:
    """잡 엔트리포인트 스크립트를 EMR_JOBS_DIR에 올리고 경로를 반환한다."""
    dest = EMR_JOBS_DIR / "scripts" / f"{job_name}.py"

    if isinstance(dest, Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(local_path.read_bytes())
    else:
        dest.upload_from(local_path)

    return str(dest)


def run_spark_job(
    job_name: str,
    entry_point_script: Path,
    entry_point_args: list[str],
) -> None:
    """EMR Serverless에 Spark 잡을 제출하고 끝날 때까지 기다린다.

    실패(FAILED/CANCELLED)면 예외를 던져 Airflow가 기존 재시도/Slack
    실패 알림 경로를 그대로 타게 한다.
    """
    client = boto3.client("emr-serverless", region_name=AWS_REGION)

    src_bundle_s3 = _upload_src_bundle()
    entry_point_s3 = _upload_script(entry_point_script, job_name)

    logger.info(f"EMR Serverless 잡 제출: {job_name}")

    response = client.start_job_run(
        applicationId=EMR_APPLICATION_ID,
        executionRoleArn=EMR_JOB_ROLE_ARN,
        name=job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point_s3,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": f"--py-files {src_bundle_s3}",
            }
        },
    )

    job_run_id = response["jobRunId"]

    logger.info(f"EMR Serverless 잡 실행 중: {job_name} (jobRunId={job_run_id})")

    while True:
        time.sleep(_POLL_INTERVAL_SECONDS)

        job_run = client.get_job_run(
            applicationId=EMR_APPLICATION_ID,
            jobRunId=job_run_id,
        )["jobRun"]

        state = job_run["state"]

        if state in _TERMINAL_STATES:
            break

    if state != "SUCCESS":
        raise RuntimeError(
            f"EMR Serverless 잡 실패: {job_name} "
            f"(jobRunId={job_run_id}, state={state}, "
            f"detail={job_run.get('stateDetails')})"
        )

    logger.info(f"EMR Serverless 잡 완료: {job_name} (jobRunId={job_run_id})")


def read_json_result(s3_path: str):
    """잡이 저장해둔 JSON 결과 파일을 읽는다."""
    return json.loads(S3Path(s3_path).read_text())
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/common/test_emr_serverless.py -v`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/common/emr_serverless.py tests/common/test_emr_serverless.py
git commit -m "feat: EMR Serverless Spark job 제출/대기 헬퍼 추가"
```

---

### Task 5: `.env.example`에 관련 변수 추가

**Files:**
- Modify: `.env.example`

- [ ] **Step 1: 변수 추가**

`.env.example` 끝에 추가:

```
# EMR Serverless — 세그먼트 지표 파이프라인 Spark 잡 실행 대상
EMR_APPLICATION_ID=
EMR_JOB_ROLE_ARN=

# DynamoDB 테이블명 (기본값 SegmentMetricsType1/2를 그대로 쓰면 비워둬도 됨)
DYNAMODB_TABLE_TYPE1=
DYNAMODB_TABLE_TYPE2=
```

- [ ] **Step 2: Commit**

```bash
git add .env.example
git commit -m "docs: .env.example에 EMR/DynamoDB 변수 추가"
```

---

### Task 6: `docker-compose.yml`에 `dynamodb-local` 서비스 추가

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: 컨테이너명 `traffic-dynamodb-local`, 포트 `8000`, 네트워크 `airflow-network`

- [ ] **Step 1: 서비스 블록 추가**

`docker-compose.yml`의 `redis` 서비스 블록 바로 뒤(healthcheck 다음 빈 줄 뒤)에 추가:

```yaml
  # =========================================================
  # DynamoDB Local
  # 로컬 개발/테스트용 — 세그먼트 지표 서빙 저장소를 AWS 자격증명 없이
  # 재현한다. APP_ENV=local일 때 src/common/config.py의
  # DYNAMODB_ENDPOINT_URL이 이 서비스를 가리킨다.
  # =========================================================
  dynamodb-local:
    image: amazon/dynamodb-local:latest
    container_name: traffic-dynamodb-local

    restart: unless-stopped

    command: >
      -jar DynamoDBLocal.jar -sharedDb -inMemory

    ports:
      - "8000:8000"

    networks:
      - airflow-network
```

- [ ] **Step 2: docker-compose 문법 확인**

Run: `docker compose config --quiet`
Expected: 에러 없이 종료 (출력 없음)

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: 로컬 개발용 dynamodb-local 서비스 추가"
```

---

### Task 7: `scripts/create_dynamodb_tables.py` — 테이블 생성 스크립트

**Files:**
- Create: `scripts/create_dynamodb_tables.py`

**Interfaces:**
- Consumes: `config.DYNAMODB_TABLE_TYPE1`, `config.DYNAMODB_TABLE_TYPE2`, `common.dynamodb.get_dynamodb_resource`
- Produces: `create_table_if_not_exists(table_name: str) -> None`, `main() -> None`

- [ ] **Step 1: 스크립트 작성**

`scripts/create_dynamodb_tables.py`:

```python
"""
DynamoDB 테이블 생성 스크립트 (idempotent)

배포 시 한 번 실행한다. 이미 테이블이 있으면 건너뛴다.

    python scripts/create_dynamodb_tables.py
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from src.common.config import DYNAMODB_TABLE_TYPE1, DYNAMODB_TABLE_TYPE2
from src.common.dynamodb import get_dynamodb_resource
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="create_dynamodb_tables")


def create_table_if_not_exists(table_name: str) -> None:
    resource = get_dynamodb_resource()

    existing = [t.name for t in resource.tables.all()]
    if table_name in existing:
        logger.info(f"이미 존재하는 테이블, 건너뜀: {table_name}")
        return

    try:
        table = resource.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "segment_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "segment_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        logger.info(f"테이블 생성 완료: {table_name}")
    except ClientError:
        logger.exception(f"테이블 생성 실패: {table_name}")
        raise


def main() -> None:
    create_table_if_not_exists(DYNAMODB_TABLE_TYPE1)
    create_table_if_not_exists(DYNAMODB_TABLE_TYPE2)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 로컬 dynamodb-local로 동작 확인**

Run:
```bash
docker compose up -d dynamodb-local
APP_ENV=local AWS_REGION=us-east-1 python scripts/create_dynamodb_tables.py
APP_ENV=local AWS_REGION=us-east-1 python scripts/create_dynamodb_tables.py
```
Expected: 첫 실행은 두 테이블 다 "테이블 생성 완료" 로그, 두 번째 실행은 둘 다 "이미 존재하는 테이블, 건너뜀" 로그 (idempotent 확인)

- [ ] **Step 3: Commit**

```bash
git add scripts/create_dynamodb_tables.py
git commit -m "feat: DynamoDB 테이블 생성 스크립트 추가"
```

---

### Task 8: `scripts/seed_dynamodb_defaults.py` — GLOBAL 기본값 시딩 스크립트

**Files:**
- Create: `scripts/seed_dynamodb_defaults.py`

**Interfaces:**
- Consumes: `config.DYNAMODB_TABLE_TYPE1/2`, `config.GLOBAL_PARTITION_KEY`, `config.DEFAULT_SORT_KEY`, `common.dynamodb.put_item`
- Produces: `seed_defaults(type1_default: int, type2_default: int) -> None`, `main() -> None`

이 스크립트가 심는 값은 설계 문서 7절 fallback 체인의 3단계(`GLOBAL#DEFAULT`)다 — 파이프라인이 한 번도 안 돌았어도 이 값이 존재해야 "무조건 응답"이 보장된다. 기본값 자체(초 단위 시간, feet 단위 길이)는 팀이 실측 후 조정할 정성적 초안이다.

- [ ] **Step 1: 스크립트 작성**

`scripts/seed_dynamodb_defaults.py`:

```python
"""
DynamoDB GLOBAL 기본값 시딩 스크립트

fallback 체인의 마지막 안전망(설계 문서 7절 3단계)이다. 파이프라인이 한
번도 성공적으로 안 돌았어도 이 값이 있어야 API가 "무조건 응답"할 수
있으므로, 파이프라인 코드가 아니라 배포 시점에 이 스크립트로 수동 시딩한다.

    python scripts/seed_dynamodb_defaults.py

기본값은 TODO(팀 검토 필요): 실측 데이터 없이 잡은 정성적 초안이다.
"""

from __future__ import annotations

from src.common.config import (
    DEFAULT_SORT_KEY,
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    GLOBAL_PARTITION_KEY,
)
from src.common.dynamodb import put_item
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="seed_dynamodb_defaults")

# TODO(팀 검토 필요): NYC 평균 도로 세그먼트 기준 정성적 초안.
DEFAULT_TYPE1_SECONDS = 45
DEFAULT_TYPE2_LENGTH_FT = 300


def seed_defaults(
    type1_default: int = DEFAULT_TYPE1_SECONDS,
    type2_default: int = DEFAULT_TYPE2_LENGTH_FT,
) -> None:
    put_item(
        DYNAMODB_TABLE_TYPE1,
        {"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY, "value": type1_default},
    )
    logger.info(f"type1 GLOBAL#DEFAULT 시딩 완료: value={type1_default}")

    put_item(
        DYNAMODB_TABLE_TYPE2,
        {"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY, "value": type2_default},
    )
    logger.info(f"type2 GLOBAL#DEFAULT 시딩 완료: value={type2_default}")


def main() -> None:
    seed_defaults()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 로컬 확인**

Run:
```bash
APP_ENV=local AWS_REGION=us-east-1 python scripts/seed_dynamodb_defaults.py
```
Expected: 두 줄 모두 "시딩 완료" 로그 출력, 에러 없음

- [ ] **Step 3: Commit**

```bash
git add scripts/seed_dynamodb_defaults.py
git commit -m "feat: DynamoDB GLOBAL 기본값 시딩 스크립트 추가"
```

---

## Self-Review

**Spec coverage**: 설계 문서 6절(DynamoDB 선택 이유/스키마)의 물리적 전제(타입별 별도 테이블, PK/SK 구조) → Task 1/7. 7절 fallback 체인의 3단계(GLOBAL#DEFAULT 수동 시딩) → Task 8. 8절의 EMR Serverless 실행 전제 → Task 4. 로컬 개발 환경(설계 문서에는 명시 안 됐지만 팀에 RDS Multi-AZ 등 HA 운영 경험이 없다는 전제와 일관되게, 개발자가 AWS 없이도 테스트할 수 있어야 함) → Task 6.

**Placeholder scan**: `DEFAULT_TYPE1_SECONDS`/`DEFAULT_TYPE2_LENGTH_FT`에 TODO 표시를 남겼는데, 이건 "미구현"이 아니라 실제 동작하는 값(45초, 300ft)에 "팀 검토가 필요한 정성적 초안"이라는 라벨을 붙인 것 — 코드베이스 기존 관례(`config.py`의 `LAPLACE_SMOOTHING_ALPHA` 등)와 동일한 패턴이라 문제 없음.

**Type consistency**: `batch_get_items`가 반환하는 키 튜플 `(segment_id, sk)`은 Task 3 테스트와 구현이 일치. 이후 API 플랜(Plan 4)이 이 시그니처를 그대로 소비한다.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-21-segment-metrics-foundation.md`. 이어서 나머지 3개 플랜(type2 길이 파이프라인, type1 시간 파이프라인, 서빙 API)도 작성 중입니다 — 4개 플랜이 모두 준비되면 실행 방식(서브에이전트 vs 인라인)을 한 번에 정하시죠.
