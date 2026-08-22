# Segment Metrics Serving API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 내비게이션 클라이언트가 세그먼트 리스트 + type + time으로 조회하면, 계층적 fallback을 거쳐 "무조건" 값을 반환하는 FastAPI 서빙 API를 만든다.

**Architecture:** 라우팅(`src/serving/nav_api.py`)은 얇게, 실제 조회/fallback 로직(`src/serving/nav_lookup.py`)에 위임한다(기존 Traffic Score API 패턴과 동일). fallback은 "키 없음"과 "DynamoDB 호출 자체 실패" 모두 동일하게 다음 단계로 넘어간다: 정확한 버킷 값 → (type1만) 세그먼트 AVG → GLOBAL#DEFAULT → 코드 상수(외부 호출 없음, 최후의 보루).

**Tech Stack:** FastAPI, Pydantic, boto3(Foundation의 `src/common/dynamodb.py` 경유), pytest + httpx(TestClient)

## Global Constraints

- 설계 문서: `docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md` 7·9절. Foundation 플랜(`src/common/dynamodb.py`)이 먼저 완료됐다고 가정한다.
- API 계약은 설계 문서 9절과 정확히 일치해야 한다: `POST /segments/values`, 요청 `{segment_ids, type, time}`, 응답 `{values}`(요청 순서 보존, 중복 segment_id도 순서대로 그대로 반환).
- fallback 체인은 4단계 고정: 정확 버킷 값 → AVG(type1만) → GLOBAL#DEFAULT → 코드 상수. 이 순서를 바꾸거나 단계를 건너뛰지 않는다.
- 이 API는 절대 조회 경로에서 계산(나눗셈 등)을 하지 않는다 — 이미 계산된 값을 읽기만 한다(설계 문서 6절 "계산은 배치에서 끝내고 서빙은 조회만"과 동일한 원칙).

---

## File Structure

- Create: `src/serving/nav_lookup.py` — time→bucket 변환, fallback 체인 로직
- Create: `src/serving/nav_api.py` — FastAPI 앱
- Modify: `docker-compose.yml` — `nav-api` 서비스 추가
- Modify: `requirements.txt` — `httpx` 추가(FastAPI TestClient), stale 주석 정리
- Create: `tests/serving/test_nav_lookup.py`, `tests/serving/test_nav_api.py`

---

### Task 1: `src/serving/nav_lookup.py` — fallback 체인 로직

**Files:**
- Create: `src/serving/nav_lookup.py`
- Test: `tests/serving/test_nav_lookup.py`

**Interfaces:**
- Consumes: `common.dynamodb.batch_get_items`, `config.{DYNAMODB_TABLE_TYPE1, DYNAMODB_TABLE_TYPE2, BUCKET_MINUTES, GLOBAL_PARTITION_KEY, DEFAULT_SORT_KEY, AVG_SORT_KEY}`
- Produces: `time_to_bucket(time_str: str) -> str`, `table_for_type(type_: int) -> str`, `resolve_segment_values(segment_ids: list[str], type_: int, time_str: str) -> list[int]`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/serving/__init__.py`(빈 파일) 생성 후 `tests/serving/test_nav_lookup.py`:

```python
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.serving import nav_lookup


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.DYNAMODB_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.DYNAMODB_TABLE_TYPE2


def _create_table(table_name, region="us-east-1"):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
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


@mock_aws
def test_resolve_uses_exact_bucket_value_when_present():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "1200", "value": 30})

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]


@mock_aws
def test_resolve_falls_back_to_avg_when_bucket_missing_type1():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE1, {"segment_id": "1", "sk": "AVG", "value": 40})

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


@mock_aws
def test_resolve_falls_back_to_global_default_when_nothing_for_segment():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE1)
    from src.common.dynamodb import put_item

    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE1,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 45},
    )

    result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [45]


@mock_aws
def test_resolve_type2_has_no_avg_tier_goes_straight_to_default():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE2,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 300},
    )
    # type2는 sk가 항상 "LENGTH"라, "AVG" 항목이 있어도 안 쓰여야 한다.
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "AVG", "value": 999})

    result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [300]


def test_resolve_falls_back_to_hardcoded_constant_when_dynamodb_unreachable():
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 2


@mock_aws
def test_resolve_preserves_order_and_duplicates():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "LENGTH", "value": 100})
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "2", "sk": "LENGTH", "value": 200})

    result = nav_lookup.resolve_segment_values(["2", "1", "2"], 2, "12:00")

    assert result == [200, 100, 200]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/serving/test_nav_lookup.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.serving.nav_lookup'`

- [ ] **Step 3: `src/serving/nav_lookup.py` 구현**

```python
"""
세그먼트 지표 조회 + fallback 체인

"무조건 응답"(설계 문서 7절)을 구현하는 핵심 모듈. 키가 없는 경우와
DynamoDB 호출 자체가 실패(예외)하는 경우를 구분하지 않고 똑같이 다음
fallback 단계로 넘어간다.

체인 순서(고정):
  1. 정확한 (segment_id, bucket) 값
  2. (type1만) (segment_id, "AVG")
  3. (GLOBAL, "DEFAULT") — 배포 시점에 수동으로 심어둔 고정값
  4. 코드 상수 — 외부 호출이 전혀 없는 최후의 보루
"""

from __future__ import annotations

from src.common.config import (
    AVG_SORT_KEY,
    BUCKET_MINUTES,
    DEFAULT_SORT_KEY,
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    GLOBAL_PARTITION_KEY,
)
from src.common.dynamodb import batch_get_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_lookup")

# DynamoDB/GLOBAL#DEFAULT까지 전부 실패했을 때 쓰는 최후의 상수. 외부 호출이
# 전혀 없어 DynamoDB 리전 자체가 완전히 응답 불가능한 상황에서도 동작한다.
# TODO(팀 검토 필요): scripts/seed_dynamodb_defaults.py의 기본값과 동일한
# 정성적 초안.
_HARDCODED_DEFAULTS = {1: 45, 2: 300}


def time_to_bucket(time_str: str) -> str:
    """'HH:MM' -> 'HHMM' (30분 단위로 내림)."""
    hour_str, minute_str = time_str.split(":")
    bucket_minute = (int(minute_str) // BUCKET_MINUTES) * BUCKET_MINUTES
    return f"{int(hour_str):02d}{bucket_minute:02d}"


def table_for_type(type_: int) -> str:
    if type_ == 1:
        return DYNAMODB_TABLE_TYPE1
    if type_ == 2:
        return DYNAMODB_TABLE_TYPE2
    raise ValueError(f"알 수 없는 type: {type_}")


def _resolve_tier(resolved: dict[str, int], ids: list[str], table_name: str, sk: str) -> None:
    """아직 못 찾은 segment_id들에 대해 (segment_id, sk) 키로 조회해서
    찾은 만큼 resolved에 채운다. DynamoDB 호출 자체가 실패해도 예외를
    삼키고 로그만 남긴다(호출부가 다음 fallback 단계로 계속 진행)."""
    if not ids:
        return

    keys = [{"segment_id": sid, "sk": sk} for sid in ids]

    try:
        items = batch_get_items(table_name, keys)
    except Exception:
        logger.exception(f"DynamoDB batch_get_items 실패: table={table_name} sk={sk}")
        return

    for sid in ids:
        item = items.get((sid, sk))
        if item is not None:
            resolved[sid] = int(item["value"])


def _lookup_global_default(table_name: str, type_: int) -> int:
    try:
        items = batch_get_items(table_name, [{"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY}])
        item = items.get((GLOBAL_PARTITION_KEY, DEFAULT_SORT_KEY))
        if item is not None:
            return int(item["value"])
    except Exception:
        logger.exception(f"DynamoDB GLOBAL#DEFAULT 조회 실패: table={table_name}")

    logger.warning(f"GLOBAL#DEFAULT까지 실패 -> 코드 상수 사용: type={type_}")
    return _HARDCODED_DEFAULTS[type_]


def resolve_segment_values(segment_ids: list[str], type_: int, time_str: str) -> list[int]:
    """요청받은 segment_ids 순서(중복 포함)대로 값을 반환한다. 항상 길이가
    같은 리스트를 반환한다 — 예외를 던지지 않는다."""

    table_name = table_for_type(type_)
    bucket = time_to_bucket(time_str)

    unique_ids = list(dict.fromkeys(segment_ids))
    resolved: dict[str, int] = {}

    # 1단계: 정확한 버킷 값
    _resolve_tier(resolved, unique_ids, table_name, bucket)

    remaining = [sid for sid in unique_ids if sid not in resolved]

    # 2단계(type1만): 세그먼트 전체 평균
    if remaining and type_ == 1:
        _resolve_tier(resolved, remaining, table_name, AVG_SORT_KEY)
        remaining = [sid for sid in unique_ids if sid not in resolved]

    # 3~4단계: GLOBAL#DEFAULT, 없으면 코드 상수
    if remaining:
        default_value = _lookup_global_default(table_name, type_)
        for sid in remaining:
            resolved[sid] = default_value

    return [resolved[sid] for sid in segment_ids]
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/serving/test_nav_lookup.py -v`
Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/serving/nav_lookup.py tests/serving/
git commit -m "feat: 세그먼트 지표 조회 fallback 체인(무조건 응답) 구현"
```

---

### Task 2: `src/serving/nav_api.py` — FastAPI 앱

**Files:**
- Create: `src/serving/nav_api.py`
- Test: `tests/serving/test_nav_api.py`
- Modify: `requirements.txt`(httpx 추가)

**Interfaces:**
- Consumes: `serving.nav_lookup.resolve_segment_values`
- Produces: FastAPI app 인스턴스 `app`, 엔드포인트 `POST /segments/values`

- [ ] **Step 1: httpx 추가 (FastAPI TestClient에 필요)**

`requirements.txt`의 `fastapi`/`uvicorn` 줄을 다음으로 교체:

```
# 세그먼트 지표 서빙 API(src/serving/nav_api.py)가 씀.
fastapi
uvicorn
PyYAML

# FastAPI TestClient(starlette.testclient)가 내부적으로 사용 — 테스트 전용.
httpx
```

- [ ] **Step 2: 실패하는 테스트 작성**

`tests/serving/test_nav_api.py`:

```python
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.serving.nav_api import app

client = TestClient(app)


def test_get_segment_values_returns_values_in_order():
    with patch("src.serving.nav_api.resolve_segment_values", return_value=[30, 50]) as mock_resolve:
        response = client.post(
            "/segments/values",
            json={"segment_ids": ["1", "2"], "type": 1, "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {"values": [30, 50]}
    mock_resolve.assert_called_once_with(["1", "2"], 1, "12:00")


def test_get_segment_values_rejects_invalid_type():
    response = client.post(
        "/segments/values",
        json={"segment_ids": ["1"], "type": 3, "time": "12:00"},
    )

    assert response.status_code == 422


def test_get_segment_values_rejects_malformed_time():
    response = client.post(
        "/segments/values",
        json={"segment_ids": ["1"], "type": 1, "time": "not-a-time"},
    )

    assert response.status_code == 422


def test_get_segment_values_rejects_empty_segment_list():
    response = client.post(
        "/segments/values",
        json={"segment_ids": [], "type": 1, "time": "12:00"},
    )

    assert response.status_code == 422
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `pytest tests/serving/test_nav_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.serving.nav_api'`

- [ ] **Step 4: `src/serving/nav_api.py` 구현**

```python
"""
서빙 API — 세그먼트 지표 조회

라우팅은 얇게 두고, 실제 조회/fallback 로직은 src/serving/nav_lookup.py에
위임한다.

로컬 실행: uvicorn src.serving.nav_api:app --reload --port 8001
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.common.logger import get_logger
from src.serving.nav_lookup import resolve_segment_values

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_api")

app = FastAPI(title="Segment Metrics API")


class SegmentValuesRequest(BaseModel):
    segment_ids: list[str] = Field(min_length=1)
    type: Literal[1, 2]
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")


class SegmentValuesResponse(BaseModel):
    values: list[int]


@app.exception_handler(Exception)
async def log_unexpected_exception(request: Request, exc: Exception):
    """개별 엔드포인트가 처리 못한 예외만 여기로 옴 — 500으로 감춰지기 전에 로그.

    fallback 체인 자체는 예외를 던지지 않으므로, 이 핸들러가 동작한다는 건
    설계된 장애 대응 범위를 벗어난 진짜 버그라는 뜻이다.
    """
    logger.error("처리되지 않은 예외: %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.post("/segments/values", response_model=SegmentValuesResponse)
def get_segment_values(request: SegmentValuesRequest) -> SegmentValuesResponse:
    values = resolve_segment_values(request.segment_ids, request.type, request.time)
    return SegmentValuesResponse(values=values)
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `pytest tests/serving/test_nav_api.py -v`
Expected: `4 passed`

- [ ] **Step 6: Commit**

```bash
git add src/serving/nav_api.py tests/serving/test_nav_api.py requirements.txt
git commit -m "feat: 세그먼트 지표 서빙 FastAPI 앱 추가"
```

---

### Task 3: `docker-compose.yml`에 `nav-api` 서비스 추가

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: 컨테이너명 `traffic-nav-api`, 포트 `8001`

- [ ] **Step 1: 서비스 블록 추가**

`docker-compose.yml`의 `traffic-score-api` 서비스 블록 뒤에 추가(기존 서비스와 동일한 패턴 — Airflow 컴포넌트가 아니라 별도 서비스):

```yaml
  # =========================================================
  # Segment Metrics API
  #
  # 내비게이션이 쓰는 세그먼트 지표(시간/길이) 조회 API. DynamoDB만 보고
  # 응답하므로 postgres/redis/celery 불필요.
  # =========================================================
  nav-api:
    <<: *airflow-image
    build:
      context: .
      dockerfile: Dockerfile

    container_name: traffic-nav-api

    restart: unless-stopped

    environment:
      PYTHONPATH: /opt/airflow
      APP_ENV: ${APP_ENV}
      AWS_REGION: ${AWS_REGION}
      DYNAMODB_TABLE_TYPE1: ${DYNAMODB_TABLE_TYPE1}
      DYNAMODB_TABLE_TYPE2: ${DYNAMODB_TABLE_TYPE2}

    volumes:
      - ./src:/opt/airflow/src

    working_dir: /opt/airflow

    ports:
      - "8001:8001"

    entrypoint: []
    command: >
      uvicorn src.serving.nav_api:app --host 0.0.0.0 --port 8001

    depends_on:
      - dynamodb-local

    networks:
      - airflow-network
```

- [ ] **Step 2: docker-compose 문법 확인**

Run: `docker compose config --quiet`
Expected: 에러 없이 종료

- [ ] **Step 3: 로컬 기동 확인**

Run:
```bash
docker compose up -d dynamodb-local nav-api
sleep 3
curl -s -X POST http://localhost:8001/segments/values \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["1"], "type": 1, "time": "12:00"}'
```
Expected: `SegmentMetricsType1` 테이블이 로컬에 없으므로(아직 `scripts/create_dynamodb_tables.py`를 안 돌렸다면) DynamoDB 호출이 실패해도 fallback 4단계(코드 상수)로 `{"values": [45]}`가 반환됨 — 즉 테이블이 없어도 API가 죽지 않는다는 것 자체가 "무조건 응답" 설계의 검증

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: 세그먼트 지표 서빙 API 로컬 서비스(nav-api) 추가"
```

---

## Self-Review

**Spec coverage**: 설계 문서 9절(API 계약, 엔드포인트/요청/응답 스키마, BatchGetItem 청크, fallback 위임) → Task1-2. 7절(4단계 fallback, 키없음/에러 동일 취급, GLOBAL#DEFAULT, 코드 상수) → Task1의 `resolve_segment_values`/`_resolve_tier`/`_lookup_global_default`. "계산은 배치에서 끝내고 서빙은 조회만"(6절) → `nav_lookup.py`에 나눗셈/집계 코드가 전혀 없음(값을 그대로 읽어 int 캐스팅만 함)으로 자연스럽게 지켜짐.

**Placeholder scan**: 없음. `_HARDCODED_DEFAULTS` 값에 TODO 라벨이 있으나 실제 동작하는 구체값.

**Type consistency**: `resolve_segment_values`가 반환하는 `list[int]`은 `SegmentValuesResponse.values: list[int]`와 일치. `time_to_bucket`이 만드는 `"HHMM"` 포맷은 Foundation Task1의 버킷 키 포맷과 일치. `_HARDCODED_DEFAULTS`와 `scripts/seed_dynamodb_defaults.py`의 기본값(45/300)이 동일한 값으로 맞춰져 있음(우연 아님 — 둘 다 fallback의 마지막 두 단계이므로 값이 같아야 사용자 경험이 일관됨).

**남은 범위 밖 항목(설계 문서 6절 언급, 이 플랜에선 다루지 않음)**: 실제 AWS ECS Fargate/ALB 프로비저닝(Terraform 등 IaC) — 이 플랜은 애플리케이션 코드와 로컬 docker-compose 검증까지만 다룬다. 프로덕션 배포 인프라는 별도 플랜/작업으로 분리하는 게 적절하다.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-21-segment-metrics-api.md`.

이제 4개 플랜이 모두 준비됐습니다:
1. `2026-08-21-segment-metrics-foundation.md` — 공통 기반
2. `2026-08-21-segment-length-pipeline.md` — type2(길이)
3. `2026-08-21-segment-time-pipeline.md` — type1(시간)
4. `2026-08-21-segment-metrics-api.md` — 서빙 API

실행 순서: 1(공통 기반) → 2·3(팀원별로 병렬 가능) → 4(서빙 API, 2·3의 DynamoDB 쓰기 스키마에 의존하지만 fallback 로직 자체는 목/moto로 독립 개발·테스트 가능).

각 플랜을 어떻게 실행할까요?

**1. Subagent-Driven (추천)** - 태스크마다 새 서브에이전트를 붙여서 리뷰하며 진행
**2. Inline Execution** - 이 세션에서 태스크 단위 배치 실행 + 체크포인트

어떤 방식으로 갈까요?
