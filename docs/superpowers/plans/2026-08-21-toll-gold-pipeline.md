# 통행료(도로+혼잡) 골드 데이터셋 파이프라인 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 택시 전용 네비게이션 API에 쓸 통행료 골드 데이터셋 두 가지(type=4 혼잡통행료, type=5 도로 통행료)를 Bronze(S3)부터 만들어서 DynamoDB로 서빙한다.

**Architecture:** Bronze(S3 — 수동 관리 요금표 YAML + MTA CBD Geofence 폴리곤 + 다리/터널 시설 목록) → Silver2(LION segment geometry와 시설명/폴리곤을 매칭해서 "이 segment가 어느 시설/zone에 해당하는가" 매핑 생성, pandas/geopandas) → Gold(매핑 × 요금표 결합해서 최종 값 계산, DynamoDB에 적재, Asset 트리거) → 서빙(DynamoDB 조회 함수, 시설/zone 밖은 0). 요금표는 1년에 한 번 정도만 바뀌므로 별도 월간 DAG가 공식 요금 페이지의 변경 여부만 감지해서 Slack 알림을 보낸다(자동 파싱/반영 없음 — 사람이 확인 후 YAML을 직접 고치고 Bronze DAG를 수동 트리거).

**Tech Stack:** Python, pandas, geopandas/shapely, boto3(DynamoDB), Airflow(Asset 기반 DAG), pytest, docker-compose(dynamodb-local).

## Global Constraints

- 타입 번호는 이미 팀 합의됨: **type=4 = 혼잡통행료, type=5 = 도로 통행료**. 다른 타입(1,2,3)과 절대 겹치지 않게 코드에도 이 숫자 그대로 쓴다.
- 이 프로젝트는 택시 전용 네비게이션 API다 — 승용차/트럭 등 다른 차종 요금은 이번 범위에 없다. 다리/터널에 택시 전용 요금이 없으면 승용차(E-ZPass) 요금을 그대로 쓴다.
- 혼잡통행료(type=4)는 맨해튼 60번가 이남(Congestion Relief Zone) 안에 있는 **모든 segment에 동일한 값**을 넣는다(진입점만 찾지 않는다) — 경로 안에 이 값이 하나라도 나오면 그 트립에 대해 한 번만 청구된다고 클라이언트가 처리하기로 했으므로, 우리는 "이 segment가 zone 안인가"만 정확하면 된다.
- 택시 혼잡통행료는 $0.75 정액, 시간대 무관, 트립마다(하루 상한 없이) 부과된다 — 승용차의 피크/오프피크/하루 1회 상한 규칙과 다르다. 따라서 이 프로젝트에서 type=4/type=5 값은 시간(date/time_slot)에 따라 달라지지 않는다 — DynamoDB sort key에 `DATE`/`SLOT`을 넣지 않고 `TYPE#{n}` 고정 키를 쓴다. (나중에 피크/오프피크 요금이 이 프로젝트에 추가되면 그때 시간 슬롯을 넣도록 확장한다 — 지금은 YAGNI.)
- 요금표/시설목록/CBD 폴리곤은 전부 **사람이 관리**한다(자동 크롤링/파싱 없음). 자동화되는 건 "공식 요금 페이지가 바뀌었는지 감지해서 알림 보내는 것"뿐이다.
- 시설/zone에 해당하지 않는 segment는 값을 쓰지 않는다(DynamoDB에 아이템 자체가 없음) — 서빙 함수가 "값 없음"을 항상 `0`으로 변환해서 반환한다("무결점 응답": null/에러 없이 항상 값 반환).
- 매 태스크 끝에 `git add`(관련 파일만, `git add -A` 금지) + commit.
- ~~MTA Central Business District Geofence의 정확한 다운로드 URL은...~~ **(해결됨)** 실제 실행 시 확인한 결과 `https://data.ny.gov/resource/srxy-5nxn.geojson`가 실제 폴리곤 좌표를 반환하는 올바른 엔드포인트다(참고로 비슷한 이름의 `vaq5-qfkz`는 geometry가 비어있는 잘못된 데이터셋이라 헷갈리지 말 것 — curl로 직접 응답 내용 확인해서 검증함).
- 요금표(`config/toll_rates.yaml`)의 금액은 이 플랜 작성 시점에 조사한 참고값이다 — **Task 2 착수 전 `mta.info`/`panynj.gov` 공식 요금표에서 실제 현재 금액을 다시 확인하고 반영할 것** (다른 상수들처럼 TODO 주석으로 표시해뒀다).

---

### Task 1: DynamoDB 로컬 개발 환경 + 공용 클라이언트

**Files:**
- Modify: `docker-compose.yml` (dynamodb-local 서비스 추가)
- Modify: `src/common/config.py` (Dynamo 관련 상수 추가)
- Create: `src/common/dynamo.py`
- Test: `tests/common/test_dynamo.py`

**Interfaces:**
- Produces: `src.common.dynamo.{NAV_GOLD_TABLE, get_resource, ensure_table, put_item, batch_write_items, get_value, batch_get_values}` (이후 모든 nav 골드 타입이 이 모듈로 DynamoDB에 쓰고 읽음 — 이번 플랜에서는 toll 도메인이 첫 소비처)

- [x] **Step 1: docker-compose.yml에 dynamodb-local 서비스 추가**

`docker-compose.yml`의 `spark-worker` 서비스와 `crash-monitor` 서비스 사이에 추가:

```yaml
  # =========================================================
  # DynamoDB Local
  # nav 골드 데이터셋(segment_id x type 조회) 서빙용. APP_ENV=local이면
  # 여기 붙고, aws면 실 DynamoDB에 붙는다(src/common/dynamo.py 참고).
  # -inMemory: 로컬 개발/테스트 DB라 재시작 시 초기화돼도 상관없음
  # (rds-local의 postgres_data 볼륨과 다르게 영속화 안 함).
  # =========================================================
  dynamodb-local:
    image: amazon/dynamodb-local:2.5.4
    container_name: traffic-dynamodb-local
    restart: unless-stopped

    command: >
      -jar DynamoDBLocal.jar -inMemory -sharedDb

    ports:
      - "8002:8000"

    networks:
      - airflow-network
```

- [x] **Step 2: src/common/config.py에 Dynamo 설정 추가**

`config.py`의 "RDS (Gold 서빙 테이블) 설정" 섹션 뒤에 추가:

```python
# ==========================
# DynamoDB (nav 골드 데이터셋 서빙) 설정
# ==========================

# nav 골드 데이터셋(segment_id x type 조회)은 RDS가 아니라 DynamoDB로
# 서빙한다 — 접근 패턴이 key-value 조회(BatchGetItem)뿐이고, 타입별로
# 갱신 주기가 달라 RDS의 write_table() 전체 replace 방식이 안 맞기
# 때문이다(자세한 배경은 docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md).
DYNAMO_REGION = os.getenv("AWS_REGION", "us-east-1")

# APP_ENV=local이면 docker-compose의 dynamodb-local(호스트 포트 8002)에
# 붙는다. 컨테이너 안에서 도는 스크립트/DAG는 LOCAL_RDS_HOST와 동일한
# 이유로 서비스명("dynamodb-local")을 써야 하므로 환경변수로 덮어쓸 수
# 있게 둔다.
DYNAMO_LOCAL_ENDPOINT = os.getenv("DYNAMO_LOCAL_ENDPOINT", "http://localhost:8002")

NAV_GOLD_TABLE = "nav_gold_values"
```

- [x] **Step 3: 테스트 폴더 생성**

```bash
mkdir -p tests/common
```

- [x] **Step 4: 실패하는 테스트 작성**

`tests/common/test_dynamo.py`:
```python
import pytest

from src.common import dynamo

TEST_TABLE = "test_nav_gold_values"


@pytest.fixture(autouse=True)
def _clean_table():
    dynamo.ensure_table(TEST_TABLE)
    table = dynamo.get_resource().Table(TEST_TABLE)
    yield
    # 각 테스트 끝나고 넣은 아이템 지우기 (테이블 자체는 재사용)
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan["Items"]:
            batch.delete_item(Key={"segment_id": item["segment_id"], "sk": item["sk"]})


def test_put_item_and_get_value():
    dynamo.put_item({"segment_id": "S1", "sk": "TYPE#4", "value": 0.75}, table_name=TEST_TABLE)

    result = dynamo.get_value("S1", "TYPE#4", table_name=TEST_TABLE)

    assert result == 0.75


def test_get_value_returns_default_when_missing():
    result = dynamo.get_value("NO_SUCH_SEGMENT", "TYPE#4", table_name=TEST_TABLE, default=0)

    assert result == 0


def test_batch_write_and_batch_get_values_preserves_order():
    dynamo.batch_write_items(
        [
            {"segment_id": "S1", "sk": "TYPE#5", "value": 6.94},
            {"segment_id": "S2", "sk": "TYPE#5", "value": 10.67},
        ],
        table_name=TEST_TABLE,
    )

    result = dynamo.batch_get_values(["S2", "S1", "S_MISSING"], "TYPE#5", table_name=TEST_TABLE, default=0)

    assert result == [10.67, 6.94, 0]


def test_batch_get_values_handles_more_than_100_segments():
    items = [{"segment_id": f"S{i}", "sk": "TYPE#4", "value": 0.75} for i in range(120)]
    dynamo.batch_write_items(items, table_name=TEST_TABLE)

    segment_ids = [f"S{i}" for i in range(120)]
    result = dynamo.batch_get_values(segment_ids, "TYPE#4", table_name=TEST_TABLE, default=0)

    assert result == [0.75] * 120
```

- [x] **Step 5: 테스트 실패 확인**

먼저 dynamodb-local을 띄운다:
```bash
docker compose up -d dynamodb-local
```

Run: `APP_ENV=local pytest tests/common/test_dynamo.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.common.dynamo'`

- [x] **Step 6: src/common/dynamo.py 작성**

```python
"""
DynamoDB 서빙 모듈 — nav 골드 데이터셋(segment_id x type 조회) 전용

src/common/db.py(RDS)와 같은 위치의 서빙 레이어지만, nav 골드 데이터셋의
접근 패턴(segment_id 목록 x type 하나로 값 목록 조회)이 순수 key-value라
DynamoDB를 쓴다. 테이블 하나(NAV_GOLD_TABLE)에 모든 타입을 담고, sort key
접두사(예: "TYPE#4")로 타입을 구분한다 — 타입마다 테이블을 나누면 RDS의
write_table() 전체 replace 같은 문제(한 타입 갱신이 다른 타입을 덮어씀)가
DynamoDB에선 애초에 없다(아이템 단위 쓰기라서). 자세한 배경은
docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md 참고.
"""

from __future__ import annotations

from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from src.common.config import APP_ENV, DYNAMO_LOCAL_ENDPOINT, DYNAMO_REGION, NAV_GOLD_TABLE

_resource = None


def _floats_to_decimals(value):
    """DynamoDB(boto3)는 Python float을 못 받고 Decimal만 받는다
    (TypeError: Float types are not supported). str로 한 번 거쳐 변환해서
    이진부동소수점 오차가 Decimal로 그대로 옮겨붙는 걸 피한다."""

    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _floats_to_decimals(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_floats_to_decimals(v) for v in value]
    return value


def _decimals_to_floats(value):
    """조회 결과(Decimal)를 다시 보통 숫자로 되돌린다 — 소비처(API
    응답 등)가 Decimal을 몰라도 되게 하기 위함."""

    if isinstance(value, Decimal):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, dict):
        return {k: _decimals_to_floats(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decimals_to_floats(v) for v in value]
    return value


def get_resource():
    """DynamoDB 리소스를 반환한다(프로세스당 한 번만 생성해서 재사용).
    APP_ENV=local이면 dynamodb-local에, 아니면 실 DynamoDB에 붙는다."""

    global _resource

    if _resource is None:
        if APP_ENV == "local":
            _resource = boto3.resource(
                "dynamodb",
                region_name=DYNAMO_REGION,
                endpoint_url=DYNAMO_LOCAL_ENDPOINT,
                aws_access_key_id="local",
                aws_secret_access_key="local",
            )
        else:
            _resource = boto3.resource("dynamodb", region_name=DYNAMO_REGION)

    return _resource


def ensure_table(table_name: str = NAV_GOLD_TABLE) -> None:
    """테이블이 없으면 만든다. 로컬 개발/테스트 편의용이다 — 실 AWS
    테이블은 미리 만들어두고 운영 중에는 이 함수를 안 쓴다(실수로 스키마를
    바꾸는 걸 막기 위함)."""

    client = get_resource().meta.client
    if table_name in client.list_tables()["TableNames"]:
        return

    table = get_resource().create_table(
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


def put_item(item: dict, table_name: str = NAV_GOLD_TABLE) -> None:
    """아이템 하나를 쓴다. item은 최소 {segment_id, sk, value}를 포함해야 한다."""

    get_resource().Table(table_name).put_item(Item=_floats_to_decimals(item))


def batch_write_items(items: list[dict], table_name: str = NAV_GOLD_TABLE) -> None:
    """여러 아이템을 배치로 쓴다. boto3의 batch_writer가 25개 단위로
    알아서 나눠 보내고 실패 시 재시도한다."""

    table = get_resource().Table(table_name)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=_floats_to_decimals(item))


def get_value(segment_id: str, sk: str, table_name: str = NAV_GOLD_TABLE, default=0):
    """(segment_id, sk) 하나를 조회한다. 없으면 default를 반환한다 —
    nav 골드 데이터셋의 "무결점 응답" 원칙: 값이 없어도, 심지어 테이블
    자체가 아직 안 만들어졌어도(Gold 파이프라인이 한 번도 안 돈 경우 등)
    절대 None/에러를 반환하지 않는다."""

    try:
        response = get_resource().Table(table_name).get_item(Key={"segment_id": segment_id, "sk": sk})
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            return default
        raise
    item = response.get("Item")
    return _decimals_to_floats(item["value"]) if item is not None else default


def batch_get_values(
    segment_ids: list[str],
    sk: str,
    table_name: str = NAV_GOLD_TABLE,
    default=0,
) -> list:
    """segment_id 목록 + 고정 sk로 값 목록을 조회한다. 응답 순서는
    요청한 segment_ids 순서와 항상 동일하다(DynamoDB BatchGetItem 자체는
    순서를 보장 안 해서 직접 맞춰준다). 없는 segment_id는 default로 채운다.
    테이블 자체가 아직 안 만들어졌어도(get_value와 동일한 이유) 전부
    default로 채워서 반환한다.
    """

    if not segment_ids:
        return []

    table = get_resource()
    found: dict[str, object] = {}

    # BatchGetItem은 한 번에 최대 100개 키만 허용한다.
    for i in range(0, len(segment_ids), 100):
        chunk = segment_ids[i : i + 100]
        keys = [{"segment_id": sid, "sk": sk} for sid in chunk]
        try:
            response = table.meta.client.batch_get_item(
                RequestItems={table_name: {"Keys": keys}}
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise
        for item in response["Responses"][table_name]:
            found[item["segment_id"]] = _decimals_to_floats(item["value"])

    return [found.get(sid, default) for sid in segment_ids]
```

> **실행 중 발견한 이슈(Task 1 실제 실행 시 수정됨)**: boto3 DynamoDB는 Python `float`를 직접 못 받는다(`TypeError: Float types are not supported. Use Decimal types instead.`). 위 코드는 이미 `_floats_to_decimals`/`_decimals_to_floats` 변환을 반영한 버전이다 — 이후 태스크(Task 6의 `put_item`/`batch_write_items` 사용)는 이 변환이 이미 적용된 것으로 보고 그대로 쓰면 된다.

- [x] **Step 7: 테스트 통과 확인**

Run: `APP_ENV=local pytest tests/common/test_dynamo.py -v`
Expected: 4개 테스트 전부 PASS

- [x] **Step 8: 커밋**

```bash
git add docker-compose.yml src/common/config.py src/common/dynamo.py tests/common/test_dynamo.py
git commit -m "feat: nav 골드 데이터셋 서빙용 DynamoDB 로컬 인프라 + 공용 클라이언트 추가"
```

---

### Task 2: Bronze — 요금표/시설목록/CBD 폴리곤

**Files:**
- Create: `config/toll_rates.yaml`
- Create: `config/toll_facilities.yaml`
- Create: `src/toll/__init__.py` (빈 파일)
- Create: `src/toll/bronze.py`
- Test: `tests/toll/test_bronze.py`
- Create: `tests/toll/__init__.py` (빈 파일)

**Interfaces:**
- Produces: `src.toll.bronze.{CBD_GEOFENCE_URL, upload_rates, upload_facilities, upload_cbd_geofence, main}` (Task 4/5가 Bronze에 저장된 파일들을 읽음)

- [x] **Step 1: 폴더/테스트 폴더 생성**

```bash
mkdir -p src/toll tests/toll
touch src/toll/__init__.py tests/toll/__init__.py
```

- [x] **Step 2: config/toll_rates.yaml 작성**

```yaml
# 택시 전용 통행료 요금표(뉴욕 전역) — 사람이 직접 관리한다(자동 크롤링 없음).
# 공식 요금 페이지가 바뀌면 dags/toll_rate_monitor.py가 Slack으로 알려주고,
# 그때 이 파일을 직접 고친 뒤 dags/toll_bronze_pipeline.py를 수동 트리거한다.
#
# 2026-01-04 인상분 기준으로 검색 확인한 값. 택시 전용 할인 요금이 따로
# 없어서 전부 승용차(E-ZPass) 요금을 그대로 쓴다. Port Authority는 피크/
# 오프피크 요금이 다른데(예: GWB 피크 $16.79 / 오프피크 $14.79), 이번
# 설계에서 도로 통행료는 시간대 무관으로 단순화하기로 했으므로(Global
# Constraints 참고) 피크 요금 하나만 쓴다.
#
# TODO(팀 검토 필요): 반영 전 mta.info/fares-tolls/tolls,
# panynj.gov/bridges-tunnels 공식 요금표에서 최신 금액 재확인할 것.

congestion:
  # 택시/그린캡: zone(60번가 이남) 통과 트립마다 정액, 시간대 무관, 하루 상한 없음.
  taxi_flat_rate: 0.75

road:
  # facility_key는 config/toll_facilities.yaml의 키와 일치해야 한다.
  # 택시 전용 요금이 별도로 없으면 passenger(E-ZPass 승용차) 요금을 그대로 쓴다.

  # --- MTA Bridges and Tunnels (2026-01 인상 후 E-ZPass 기준) ---
  verrazzano_narrows:
    passenger: 7.46
  queens_midtown_tunnel:
    passenger: 7.46
  hugh_l_carey_tunnel:
    passenger: 7.46
  robert_f_kennedy_bridge:
    passenger: 7.46
  whitestone_bridge:
    passenger: 7.46
  throgs_neck_bridge:
    passenger: 7.46
  henry_hudson_bridge:
    passenger: 3.42
  marine_parkway_bridge:
    passenger: 2.80
  cross_bay_bridge:
    passenger: 2.80

  # --- Port Authority (2026-01 인상 후, 6개 시설 전부 동일 요금, 피크 기준) ---
  lincoln_tunnel:
    passenger: 16.79
  holland_tunnel:
    passenger: 16.79
  george_washington_bridge:
    passenger: 16.79
  bayonne_bridge:
    passenger: 16.79
  goethals_bridge:
    passenger: 16.79
  outerbridge_crossing:
    passenger: 16.79
```

- [x] **Step 3: config/toll_facilities.yaml 작성**

```yaml
# 다리/터널 시설 목록(뉴욕 전역, MTA 9개 + Port Authority 6개 = 15개) —
# LION의 Street 컬럼에서 이 패턴을 포함하는 segment를 그 시설로 분류한다
# (대소문자 무시, 부분 일치). facility_key는 config/toll_rates.yaml의
# road 아래 키와 반드시 일치해야 한다.
#
# 패턴은 전부 실제 LION GDB(ogrinfo)로 대조 검증했다 — 특히 아래는
# "다리 이름을 포함하지만 다리가 아닌" 도로가 섞여 있어 패턴을 좁혔다:
# - WHITESTONE: "WHITESTONE EXPRESSWAY"(비과금 고속도로)가 섞여있어
#   "WHITESTONE BRIDGE"로 한정
# - THROGS NECK: "THROGS NECK BOULEVARD/EXPRESSWAY"(비과금)가 섞여있어
#   "THROGS NECK BR"(BRIDGE/BRDG/BRG 접근로까지 포함, 대로/고속도로는 제외)
# - CROSS BAY: "CROSS BAY BOULEVARD/PARKWAY"(비과금)가 섞여있어
#   "CROSS BAY VET"(실제 다리 세그먼트인 VET MEM/VETERANS MEMORIAL만)
# - GOETHALS/OUTERBRIDGE/BAYONNE: 같은 이름의 무관한 AVENUE/COURT가
#   있어 BRIDGE/CROSSING/BR까지 포함해서 좁힘

# --- MTA Bridges and Tunnels (9개) ---
verrazzano_narrows:
  street_contains: "VERRAZZANO"
queens_midtown_tunnel:
  street_contains: "QUEENS MIDTOWN TUNNEL"
hugh_l_carey_tunnel:
  street_contains: "HUGH L CAREY"
robert_f_kennedy_bridge:
  street_contains: "ROBERT F KENNEDY"
whitestone_bridge:
  street_contains: "WHITESTONE BRIDGE"
throgs_neck_bridge:
  street_contains: "THROGS NECK BR"
henry_hudson_bridge:
  street_contains: "HENRY HUDSON BR"
marine_parkway_bridge:
  street_contains: "MARINE PARKWAY"
cross_bay_bridge:
  street_contains: "CROSS BAY VET"

# --- Port Authority (6개) ---
lincoln_tunnel:
  street_contains: "LINCOLN TUNNEL"
holland_tunnel:
  street_contains: "HOLLAND TUNNEL"
george_washington_bridge:
  street_contains: "GEORGE WASHINGTON BR"
bayonne_bridge:
  street_contains: "BAYONNE BR"
goethals_bridge:
  street_contains: "GOETHALS BRIDGE"
outerbridge_crossing:
  street_contains: "OUTERBRIDGE CROSSING"
```

> **실행 중 발견한 이슈**: 처음엔 대표 예시로 6개 시설만 넣었는데, 실제로는 MTA 9개+Port Authority 6개(총 15개) 전부 있어야 한다는 걸 지적받고 전체로 확장했다. GW 브리지는 "GEO WASHINGTON BR"이 아니라 "GEORGE WASHINGTON BRIDGE"(full word)로 들어있던 것도 수정. 15개 패턴 전부 `ogrinfo`로 실제 LION GDB 대조 확인 + 헷갈리는 케이스(같은 이름의 비과금 도로/무관한 거리)까지 자동화된 매칭 테스트로 검증했다.

- [x] **Step 4: 실패하는 테스트 작성**

`tests/toll/test_bronze.py`:
```python
import yaml

from src.toll.bronze import upload_facilities, upload_rates


def test_upload_rates_copies_yaml_to_bronze(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"

    out_path = upload_rates(
        source_path="config/toll_rates.yaml",
        bronze_root=bronze_root,
    )

    assert out_path.exists()
    data = yaml.safe_load(out_path.read_text())
    assert data["congestion"]["taxi_flat_rate"] == 0.75
    assert "queens_midtown_tunnel" in data["road"]


def test_upload_facilities_copies_yaml_to_bronze(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"

    out_path = upload_facilities(
        source_path="config/toll_facilities.yaml",
        bronze_root=bronze_root,
    )

    assert out_path.exists()
    data = yaml.safe_load(out_path.read_text())
    assert data["lincoln_tunnel"]["street_contains"] == "LINCOLN TUNNEL"
```

- [x] **Step 5: 테스트 실패 확인**

Run: `pytest tests/toll/test_bronze.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.bronze'`

- [x] **Step 6: src/toll/bronze.py 작성**

```python
"""
Bronze — 통행료 요금표/시설목록/CBD Geofence 폴리곤

세 가지 다 자동 수집이 아니라 사람이 관리하는 참조 데이터다(공식 요금
API가 없다 — docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md
참고). 이 파일은 로컬 config/*.yaml 파일과 CBD 폴리곤을 그대로
Bronze(S3 또는 로컬)에 올리는 역할만 한다 — 변환/파싱 없음(Bronze 원칙).

CBD Geofence는 MTA가 공개한 공식 GIS 경계 데이터를 받는다. 다운로드
URL은 카탈로그 페이지에서 실제 파일 링크를 확인한 뒤 CBD_GEOFENCE_URL을
채워 넣을 것 — 이 플랜 작성 시점엔 카탈로그 페이지만 확인됐다:
https://catalog.data.gov/dataset/mta-central-business-district-geofence-beginning-june-2024
"""

from __future__ import annotations

import shutil
from pathlib import Path

import requests

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_bronze")

SOURCE = "toll"
BRONZE_ROOT = BRONZE_DIR / SOURCE

# data.ny.gov(Socrata)의 "MTA Central Business District Geofence: Beginning
# June 2024" 데이터셋(srxy-5nxn). 비슷한 이름의 vaq5-qfkz는 geometry가
# 비어있는 잘못된 데이터셋이라 혼동하지 말 것(직접 curl로 확인함).
CBD_GEOFENCE_URL = "https://data.ny.gov/resource/srxy-5nxn.geojson"


def upload_rates(source_path: str = "config/toll_rates.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_rates.yaml을 그대로 Bronze에 올린다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_rates.yaml"
    shutil.copyfile(source_path, out_path)

    logger.info(f"[toll_bronze] 요금표 업로드 완료 -> {out_path}")
    return out_path


def upload_facilities(source_path: str = "config/toll_facilities.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_facilities.yaml을 그대로 Bronze에 올린다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_facilities.yaml"
    shutil.copyfile(source_path, out_path)

    logger.info(f"[toll_bronze] 시설목록 업로드 완료 -> {out_path}")
    return out_path


def upload_cbd_geofence(url: str = CBD_GEOFENCE_URL, bronze_root: Path = BRONZE_ROOT) -> Path:
    """MTA CBD Geofence GeoJSON을 받아서 그대로 Bronze에 저장한다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "cbd_geofence.geojson"

    logger.info(f"[toll_bronze] CBD geofence 다운로드 시작: {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)

    logger.info(f"[toll_bronze] CBD geofence 업로드 완료 -> {out_path}")
    return out_path


def main() -> None:
    upload_rates()
    upload_facilities()
    upload_cbd_geofence()


if __name__ == "__main__":
    main()
```

- [x] **Step 7: 테스트 통과 확인**

Run: `pytest tests/toll/test_bronze.py -v`
Expected: 2개 테스트 전부 PASS

- [x] **Step 8: 커밋**

```bash
git add config/toll_rates.yaml config/toll_facilities.yaml src/toll/__init__.py src/toll/bronze.py tests/toll/__init__.py tests/toll/test_bronze.py
git commit -m "feat: 통행료 요금표/시설목록/CBD 폴리곤 Bronze 업로드 추가"
```

---

### Task 3: 요금표 월간 확인 알림 DAG

> **실행 중 발견한 이슈로 설계 변경**: 원래 계획은 페이지 내용을 해시로
> 비교해서 자동으로 "바뀐 것 같다"를 판단하는 것이었다. 실제로 curl/
> Python `requests`로 mta.info를 가져와보니 브라우저 User-Agent를 완전히
> 채워도 전부 **403 Access Denied**(WAF 봇 차단)가 나서 애초에 코드로
> 가져올 방법이 없었다. 그래서 자동 변경 감지를 포기하고, 매달 무조건
> "직접 확인하라"는 알림만 보내는 것으로 단순화했다(사용자 승인받음).

**Files:**
- Create: `src/toll/rate_monitor.py`
- Test: `tests/toll/test_rate_monitor.py`
- Create: `dags/toll_rate_monitor.py`

**Interfaces:**
- Consumes: `src.common.alerts.notify_slack_message`
- Produces: `src.toll.rate_monitor.{RATE_PAGE_URLS, build_reminder_message}` (dags/toll_rate_monitor.py가 매달 이 함수를 호출)

- [x] **Step 1: 실패하는 테스트 작성**

`tests/toll/test_rate_monitor.py`:
```python
from src.toll.rate_monitor import build_reminder_message


def test_build_reminder_message_includes_all_urls():
    message = build_reminder_message(urls=["https://example.com/a", "https://example.com/b"])

    assert "https://example.com/a" in message
    assert "https://example.com/b" in message


def test_build_reminder_message_mentions_config_file():
    message = build_reminder_message(urls=["https://example.com/a"])

    assert "toll_rates.yaml" in message
```

- [x] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_rate_monitor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.rate_monitor'`

- [x] **Step 3: src/toll/rate_monitor.py 작성**

```python
"""
통행료 요금표 월간 확인 알림

원래는 공식 요금 페이지 내용을 가져와 해시로 비교해서 "바뀐 것 같다"를
자동 판단하려 했으나, 실제로 해보니 mta.info가 봇 차단(WAF)이 걸려있어서
requests/curl 어떤 방식(브라우저 User-Agent 포함)으로도 페이지를 가져올
수 없다(403 Access Denied). 그래서 자동 변경 감지는 포기하고, 매달
무조건 "직접 확인하라"는 알림만 보낸다 — 확인/판단/config/toll_rates.yaml
수정은 항상 사람이 한다.
"""

from __future__ import annotations

RATE_PAGE_URLS = [
    "https://www.mta.info/fares-tolls/tolls/vehicle-types",
    "https://www.mta.info/fares-tolls/tolls/congestion-relief-zone",
    "https://www.panynj.gov/bridges-tunnels/en/e-zpass.html",
]


def build_reminder_message(urls: list[str] = RATE_PAGE_URLS) -> str:
    """매달 보낼 Slack 알림 메시지를 만든다."""

    urls_text = "\n".join(f"- {url}" for url in urls)
    return (
        ":bell: 통행료 요금표 월간 확인 알림\n"
        f"{urls_text}\n"
        "위 페이지들을 직접 확인해서 config/toll_rates.yaml과 다르면 고친 뒤 "
        "toll_bronze_pipeline DAG를 수동 트리거하세요."
    )
```

- [x] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_rate_monitor.py -v`
Expected: 2개 테스트 전부 PASS

- [x] **Step 5: dags/toll_rate_monitor.py 작성**

```python
"""
DAG: toll_rate_monitor

통행료 요금표를 매달 확인하라고 사람에게 알려주기만 하는 DAG. mta.info가
봇 차단(WAF)이 걸려있어서 페이지 내용을 코드로 가져와 비교하는 자동 변경
감지는 불가능하다고 확인했다(src/toll/rate_monitor.py 모듈 docstring
참고) — 그래서 매달 무조건 Slack 알림을 보내고, 실제 확인/판단/
config/toll_rates.yaml 수정은 항상 사람이 한다.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from src.common.alerts import notify_slack_failure, notify_slack_message

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_slack_failure,
}


@dag(
    dag_id="toll_rate_monitor",
    description="통행료 요금표 월간 확인 알림 (자동 변경 감지 아님 — mta.info 봇 차단)",
    schedule="0 9 1 * *",  # 매달 1일 오전 9시
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "monthly"],
)
def toll_rate_monitor():

    @task(task_id="send_reminder")
    def send_reminder():
        from src.toll.rate_monitor import build_reminder_message

        notify_slack_message(build_reminder_message())

    send_reminder()


toll_rate_monitor()
```

- [x] **Step 6: 커밋**

```bash
git add src/toll/rate_monitor.py tests/toll/test_rate_monitor.py dags/toll_rate_monitor.py
git commit -m "feat: 통행료 요금표 월간 확인 알림 DAG 추가"
```

---

### Task 4: Silver2 — LION segment 추출 + 다리/터널 시설 매칭

**Files:**
- Create: `src/toll/silver2.py`
- Test: `tests/toll/test_silver2.py`

**Interfaces:**
- Consumes: `src.toll.bronze.BRONZE_ROOT`(경로 규칙), LION Bronze GDB(`data/bronze/lion/version_date=*/lion/lion.gdb`)
- Produces: `src.toll.silver2.{MAP_LION_FACILITY_PATH, load_lion_segments, match_lion_facilities, build_lion_facility_mapping}` (Task 6이 이 매핑을 읽음)

- [x] **Step 1: 실패하는 테스트 작성**

`tests/toll/test_silver2.py`:
```python
import geopandas as gpd
import pandas as pd
import yaml
from shapely.geometry import LineString

from src.toll.silver2 import match_lion_facilities


def test_match_lion_facilities_matches_by_street_substring(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1", "S2", "S3"],
        "street": ["LINCOLN TUNNEL", "5 AVENUE", "QUEENS MIDTOWN TUNNEL APPROACH"],
        "geometry": [LineString([(0, 0), (1, 1)])] * 3,
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({
        "lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"},
        "queens_midtown_tunnel": {"street_contains": "QUEENS MIDTOWN TUNNEL"},
    }))

    result = match_lion_facilities(segments, facilities_path)

    assert set(result["segment_id"]) == {"S1", "S3"}
    row_s1 = result[result["segment_id"] == "S1"].iloc[0]
    assert row_s1["facility_key"] == "lincoln_tunnel"
    row_s3 = result[result["segment_id"] == "S3"].iloc[0]
    assert row_s3["facility_key"] == "queens_midtown_tunnel"


def test_match_lion_facilities_excludes_non_matching_segments(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1"],
        "street": ["5 AVENUE"],
        "geometry": [LineString([(0, 0), (1, 1)])],
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({"lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"}}))

    result = match_lion_facilities(segments, facilities_path)

    assert result.empty
```

- [x] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.silver2'`

- [x] **Step 3: src/toll/silver2.py 작성 (이번 태스크 분량만)**

```python
"""
Silver2 — LION segment x 통행료 시설/zone 매핑

toll 도메인이 자기 계산에 필요한 LION segment 정보(segment_id, street,
geometry)를 직접 뽑아 쓴다 — lion 도메인은 현재 Bronze까지만 있고
Silver1/Gold2가 없으므로(다른 브랜치에서 재구축 예정), 이 매핑에 필요한
최소한(street 이름, geometry)만 이 파일에서 직접 GDB로부터 읽는다.

시설 매칭(다리/터널)은 street 이름 부분일치, zone 매칭(혼잡통행료 대상)은
공간조인이라 둘 다 "여러 소스를 구조적으로 연결"하는 Silver2 성격이다.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

from src.common.config import SILVER2_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_silver2")

MAP_LION_FACILITY_PATH = SILVER2_DIR / "map_toll_facility_segment.parquet"


def load_lion_segments(gdb_path: Path) -> gpd.GeoDataFrame:
    """LION Bronze GDB에서 segment_id/street/geometry만 뽑는다.

    LION 원본은 같은 segment_id가 여러 행으로 중복돼 있다(실측:
    243,237행 중 고유 segment_id는 218,373개 — 약 2.5만 건 중복). 원래
    lion/silver1.py가 이 dedup을 해줬는데 지금은 lion 도메인이 Bronze만
    있어서 이 파일에서 직접 처리한다(조용히 첫 번째 행만 남김, 기존
    lion/silver1.py와 동일한 정책)."""

    gdf = gpd.read_file(gdb_path, layer="lion")
    gdf = gdf.rename(columns={"SegmentID": "segment_id", "Street": "street"})
    gdf = gdf.drop_duplicates(subset="segment_id", keep="first")
    return gdf[["segment_id", "street", "geometry"]]


def match_lion_facilities(segments: gpd.GeoDataFrame, facilities_path: Path) -> pd.DataFrame:
    """segments의 street 컬럼이 facilities_path에 정의된 시설명 패턴을
    포함하면 그 시설로 매칭한다. 매칭 안 되는 segment는 결과에서 빠진다
    (통행료 대상 아님)."""

    facilities = yaml.safe_load(Path(facilities_path).read_text())

    rows = []
    for facility_key, rule in facilities.items():
        pattern = rule["street_contains"]
        matched = segments[segments["street"].str.contains(pattern, case=False, na=False)]
        for segment_id in matched["segment_id"]:
            rows.append({"segment_id": segment_id, "facility_key": facility_key})

    return pd.DataFrame(rows, columns=["segment_id", "facility_key"])


def build_lion_facility_mapping(
    gdb_path: Path,
    facilities_path: Path = Path("config/toll_facilities.yaml"),
    out_path: Path = MAP_LION_FACILITY_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    result = match_lion_facilities(segments, facilities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] 시설 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
```

- [x] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: 2개 테스트 전부 PASS

- [x] **Step 5: 커밋**

```bash
git add src/toll/silver2.py tests/toll/test_silver2.py
git commit -m "feat: LION segment 추출 + 다리/터널 시설명 매칭(Silver2) 추가"
```

---

### Task 5: Silver2 — CBD zone 공간 매칭

**Files:**
- Modify: `src/toll/silver2.py` (함수 추가)
- Modify: `tests/toll/test_silver2.py` (테스트 추가)

**Interfaces:**
- Produces: `src.toll.silver2.{MAP_LION_CBD_PATH, match_lion_cbd, build_lion_cbd_mapping}` (Task 6이 이 매핑을 읽음)

- [x] **Step 1: 실패하는 테스트 추가**

`tests/toll/test_silver2.py` 끝에 추가:
```python
from shapely.geometry import Polygon

from src.toll.silver2 import match_lion_cbd


def test_match_lion_cbd_keeps_segments_inside_polygon():
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}
    )
    segments = gpd.GeoDataFrame({
        "segment_id": ["INSIDE", "OUTSIDE"],
        "geometry": [
            LineString([(2, 2), (3, 3)]),      # zone 안
            LineString([(100, 100), (101, 101)]),  # zone 밖
        ],
    })

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["INSIDE"]


def test_match_lion_cbd_keeps_segments_touching_boundary():
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}
    )
    # 경계선에 걸치는 segment(zone 진입 지점)도 포함돼야 한다.
    segments = gpd.GeoDataFrame({
        "segment_id": ["BOUNDARY"],
        "geometry": [LineString([(10, 5), (15, 5)])],
    })

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["BOUNDARY"]


def test_match_lion_cbd_reprojects_when_crs_differs():
    # CBD Geofence는 위경도(EPSG:4326)로 오고 LION segment는 EPSG:2263(피트)다.
    # 좌표계가 다르면 좌표값 범위 자체가 완전히 달라서(-180~180 vs 수십만 단위)
    # 재투영 없이 조인하면 실제로 안 겹치는 걸로 나온다(실제로 겪은 버그).
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:4326",
    )
    segments = gpd.GeoDataFrame(
        {
            "segment_id": ["INSIDE"],
            "geometry": [LineString([(2, 2), (3, 3)])],
        },
        crs="EPSG:4326",
    ).to_crs("EPSG:2263")

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["INSIDE"]
```

> **실행 중 발견한 버그**: 위 테스트 중 처음 두 개(`crs=None`인 단순 GeoDataFrame)만으로는 안 잡히는 버그가 있었다 — 실제 LION(EPSG:2263)과 CBD Geofence(EPSG:4326)로 돌려보니 `gpd.sjoin`이 좌표계 불일치를 경고만 띄우고 **조용히 0건**을 반환했다(에러가 안 나서 더 위험). `test_match_lion_cbd_reprojects_when_crs_differs`로 이 케이스를 재현/고정했고, 아래 `match_lion_cbd` 구현에 CRS 재투영 로직을 추가해서 해결했다.

- [x] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_silver2.py -v -k cbd_zone`
Expected: FAIL with `ImportError: cannot import name 'match_lion_cbd'`

- [x] **Step 3: src/toll/silver2.py에 함수 추가**

`src/toll/silver2.py` 끝에 추가(import문은 파일 상단에 이미 있는 `gpd`/`pd`/`Path`/`SILVER2_DIR`/`logger` 재사용):

```python
MAP_LION_CBD_PATH = SILVER2_DIR / "map_cbd_zone_segment.parquet"


def match_lion_cbd(segments: gpd.GeoDataFrame, zone_polygon: gpd.GeoDataFrame) -> pd.DataFrame:
    """segments 중 CBD(Congestion Relief Zone) 폴리곤과 교차하는(경계에
    걸친 것 포함) segment_id만 반환한다. intersects를 쓰는 이유: zone
    "안"으로 완전히 들어간 segment뿐 아니라 zone 경계를 지나는 진입
    segment도 혼잡통행료 대상이기 때문이다(둘을 구분할 필요 없음 — 스펙
    참고: zone 내부 segment 전부에 값을 넣고 dedup은 클라이언트가 함)."""

    if segments.crs is None:
        segments = segments.set_crs(zone_polygon.crs, allow_override=True)
    elif zone_polygon.crs is not None and segments.crs != zone_polygon.crs:
        # CBD Geofence는 위경도(EPSG:4326)로 오고 LION segment는 EPSG:2263
        # (피트)이라 좌표계가 다르면 gpd.sjoin이 경고만 내고 조용히 0건을
        # 반환한다(실제로 겪음) — 반드시 같은 좌표계로 맞춰야 한다.
        zone_polygon = zone_polygon.to_crs(segments.crs)

    joined = gpd.sjoin(segments, zone_polygon, how="inner", predicate="intersects")
    return joined[["segment_id"]].drop_duplicates().reset_index(drop=True)


def build_lion_cbd_mapping(
    gdb_path: Path,
    cbd_geofence_path: Path = Path("data/bronze/toll/cbd_geofence.geojson"),
    out_path: Path = MAP_LION_CBD_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    zone_polygon = gpd.read_file(cbd_geofence_path)

    result = match_lion_cbd(segments, zone_polygon)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] CBD zone 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
```

- [x] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: 4개 테스트 전부 PASS

- [x] **Step 5: 커밋**

```bash
git add src/toll/silver2.py tests/toll/test_silver2.py
git commit -m "feat: CBD Geofence 폴리곤 x LION segment 공간 매칭(Silver2) 추가"
```

---

### Task 6: Gold — 값 계산 + DynamoDB 적재 + 서빙 조회 함수

**Files:**
- Create: `src/toll/gold.py`
- Test: `tests/toll/test_gold.py`

**Interfaces:**
- Consumes: `src.toll.silver2.{MAP_LION_FACILITY_PATH, MAP_LION_CBD_PATH}`, `src.common.dynamo.{batch_write_items, get_value}`
- Produces: `src.toll.gold.{TYPE_CONGESTION, TYPE_ROAD_TOLL, load_rate_table, build_gold_items, write_gold_items, get_toll_value}` (서빙 API가 `get_toll_value`를 호출)

- [x] **Step 1: 실패하는 테스트 작성**

`tests/toll/test_gold.py`:
```python
import pandas as pd
import yaml

from src.toll.gold import TYPE_CONGESTION, TYPE_ROAD_TOLL, build_gold_items, load_rate_table


def _write_rate_table(tmp_path):
    path = tmp_path / "toll_rates.yaml"
    path.write_text(yaml.dump({
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }))
    return path


def test_load_rate_table_reads_yaml(tmp_path):
    path = _write_rate_table(tmp_path)

    rates = load_rate_table(path)

    assert rates["congestion"]["taxi_flat_rate"] == 0.75
    assert rates["road"]["lincoln_tunnel"]["passenger"] == 17.00


def test_build_gold_items_creates_congestion_items_for_zone_segments(tmp_path):
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame({"segment_id": ["Z1", "Z2"]})
    facility_map = pd.DataFrame(columns=["segment_id", "facility_key"])

    items = build_gold_items(rate_table, zone_map, facility_map)

    congestion_items = [i for i in items if i["sk"] == f"TYPE#{TYPE_CONGESTION}"]
    assert {i["segment_id"] for i in congestion_items} == {"Z1", "Z2"}
    assert all(i["value"] == 0.75 for i in congestion_items)


def test_build_gold_items_creates_road_toll_items_with_passenger_fallback(tmp_path):
    rate_table = {
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }
    zone_map = pd.DataFrame(columns=["segment_id"])
    facility_map = pd.DataFrame({"segment_id": ["S1", "S2"], "facility_key": ["lincoln_tunnel", "lincoln_tunnel"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    road_items = [i for i in items if i["sk"] == f"TYPE#{TYPE_ROAD_TOLL}"]
    assert {i["segment_id"] for i in road_items} == {"S1", "S2"}
    assert all(i["value"] == 17.00 for i in road_items)


def test_build_gold_items_skips_facility_without_rate():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame(columns=["segment_id"])
    # rate_table에 없는 시설 -> 값을 못 만드니 결과에서 빠져야 한다.
    facility_map = pd.DataFrame({"segment_id": ["S1"], "facility_key": ["unknown_facility"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert items == []
```

- [x] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_gold.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.gold'`

- [x] **Step 3: src/toll/gold.py 작성**

```python
"""
Gold — 통행료(혼잡/도로) 최종 값 계산 + DynamoDB 적재 + 서빙 조회

Silver2의 두 매핑(zone 안 segment, 시설 매칭 segment)에 요금표(Bronze)를
결합해서 최종 (segment_id, type) -> value를 만들고 DynamoDB에 쓴다.
혼잡/도로 통행료 둘 다 시간대에 따라 안 바뀌므로(택시 정액 요금 —
스펙의 Global Constraints 참고) sort key에 DATE/SLOT을 안 넣고
"TYPE#{n}" 고정 키만 쓴다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from src.common import dynamo
from src.common.logger import get_logger
from src.toll.silver2 import MAP_LION_CBD_PATH, MAP_LION_FACILITY_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_gold")

TYPE_CONGESTION = 4
TYPE_ROAD_TOLL = 5


def load_rate_table(path: Path = Path("data/bronze/toll/toll_rates.yaml")) -> dict:
    return yaml.safe_load(Path(path).read_text())


def build_gold_items(rate_table: dict, zone_map: pd.DataFrame, facility_map: pd.DataFrame) -> list[dict]:
    """혼잡통행료(zone 안 전 segment, 정액) + 도로통행료(시설 매칭 segment,
    요금표에 있는 시설만) 아이템 목록을 만든다. 요금표에 없는 facility_key는
    (예: 시설 목록엔 있는데 아직 요금이 안 채워진 경우) 조용히 건너뛴다 —
    값을 지어내지 않는다."""

    items = []

    taxi_flat_rate = rate_table["congestion"]["taxi_flat_rate"]
    for segment_id in zone_map["segment_id"]:
        items.append({
            "segment_id": segment_id,
            "sk": f"TYPE#{TYPE_CONGESTION}",
            "value": taxi_flat_rate,
        })

    road_rates = rate_table["road"]
    for _, row in facility_map.iterrows():
        facility_key = row["facility_key"]
        if facility_key not in road_rates:
            logger.warning(f"[toll_gold] 요금표에 없는 시설 건너뜀: {facility_key}")
            continue
        items.append({
            "segment_id": row["segment_id"],
            "sk": f"TYPE#{TYPE_ROAD_TOLL}",
            "value": road_rates[facility_key]["passenger"],
        })

    return _dedupe_items(items)


def _dedupe_items(items: list[dict]) -> list[dict]:
    """(segment_id, sk) 기준으로 중복을 제거한다(첫 값 유지). DynamoDB
    batch_write_item은 같은 배치 안에 동일 키가 두 번 있으면 통째로
    에러를 낸다 — 실제로 LION 원본에 중복 segment_id 행이 있어서 겪었다
    (load_lion_segments에서 1차로 제거하지만, 여기서도 한 번 더 방어)."""

    seen: set[tuple] = set()
    deduped = []
    for item in items:
        key = (item["segment_id"], item["sk"])
        if key in seen:
            logger.warning(f"[toll_gold] 중복 키 건너뜀: {key}")
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def write_gold_items(items: list[dict]) -> None:
    dynamo.ensure_table()
    dynamo.batch_write_items(items)
    logger.info(f"[toll_gold] DynamoDB에 {len(items)}개 아이템 적재 완료")


def build_and_write(
    rate_table_path: Path = Path("data/bronze/toll/toll_rates.yaml"),
    zone_map_path: Path = MAP_LION_CBD_PATH,
    facility_map_path: Path = MAP_LION_FACILITY_PATH,
) -> int:
    rate_table = load_rate_table(rate_table_path)
    zone_map = pd.read_parquet(str(zone_map_path))
    facility_map = pd.read_parquet(str(facility_map_path))

    items = build_gold_items(rate_table, zone_map, facility_map)
    write_gold_items(items)
    return len(items)


def get_toll_value(segment_id: str, toll_type: int) -> float:
    """서빙 조회 함수. 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음)."""

    return dynamo.get_value(segment_id, f"TYPE#{toll_type}", default=0)


if __name__ == "__main__":
    build_and_write()
```

- [x] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_gold.py -v`
Expected: 4개 테스트 전부 PASS

- [x] **Step 5: get_toll_value 테스트 추가 및 확인**

`tests/toll/test_gold.py` 끝에 추가:
```python
from src.toll.gold import get_toll_value, write_gold_items


def test_get_toll_value_returns_zero_when_not_found():
    result = get_toll_value("NO_SUCH_SEGMENT", 5)

    assert result == 0


def test_get_toll_value_returns_written_value():
    write_gold_items([{"segment_id": "S99", "sk": "TYPE#5", "value": 12.34}])

    result = get_toll_value("S99", 5)

    assert result == 12.34
```

Run: `APP_ENV=local pytest tests/toll/test_gold.py -v -k get_toll_value`
Expected: 2개 테스트 전부 PASS (dynamodb-local이 떠 있어야 함: `docker compose up -d dynamodb-local`)

- [x] **Step 6: 커밋**

```bash
git add src/toll/gold.py tests/toll/test_gold.py
git commit -m "feat: 통행료 Gold 계산 + DynamoDB 적재 + 서빙 조회 함수(get_toll_value) 추가"
```

> **실행 중 발견한 버그 2개(실제 Silver2 데이터로 `build_and_write()` 돌려보다 발견)**:
> 1. **테이블 없음 예외**: `get_toll_value`/`dynamo.get_value`가 `nav_gold_values` 테이블이 아직 한 번도 안 만들어진 상태에서 호출되면 `ResourceNotFoundException`을 그대로 던졌다 — "무결점 응답" 원칙에 어긋난다. `src/common/dynamo.py`의 `get_value`/`batch_get_values`에 `ClientError`(`ResourceNotFoundException`) 캐치를 추가해서 이 경우도 `default`를 반환하게 고쳤다(Task 1 코드도 반영해뒀다).
> 2. **중복 키로 배치 쓰기 실패**: 실제 LION GDB는 같은 `segment_id`가 여러 행으로 중복돼 있다(243,237행 중 고유 218,373개, 약 2.5만 건 중복 — 예전 `lion/silver1.py`가 이 dedup을 해주던 걸 지금은 아무도 안 함). 이 때문에 `dynamo.batch_write_items`가 `ValidationException: duplicate keys`로 실패했다. `src/toll/silver2.py`의 `load_lion_segments`에 `drop_duplicates(subset="segment_id")`를 추가하고, `src/toll/gold.py`의 `build_gold_items`에도 `_dedupe_items()` 방어 로직을 추가했다.
>
> 두 수정 다 위 코드 블록에는 이미 반영 안 돼 있으니, 실제 구현 시 `src/common/dynamo.py`(Task 1)의 `get_value`/`batch_get_values`에 `ClientError` 캐치를, `src/toll/silver2.py`의 `load_lion_segments`에 dedup을 반드시 추가할 것.

---

### Task 7: Airflow DAG 연결 (Bronze DAG + Asset 트리거 Gold DAG)

**Files:**
- Create: `dags/toll_bronze_pipeline.py`
- Create: `dags/toll_silver_gold_pipeline.py`

**Interfaces:**
- Consumes: `src.toll.bronze.{upload_rates, upload_facilities, upload_cbd_geofence}`, `src.toll.silver2.{build_lion_facility_mapping, build_lion_cbd_mapping}`, `src.toll.gold.build_and_write`

- [x] **Step 1: dags/toll_bronze_pipeline.py 작성**

```python
"""
DAG: toll_bronze_pipeline

통행료 요금표/시설목록/CBD 폴리곤을 Bronze에 올린다. 요금표는 사람이
config/toll_rates.yaml을 고친 뒤에만 값이 바뀌므로 cron 스케줄이 아니라
수동 트리거(schedule=None)로 둔다 — toll_rate_monitor DAG가 변경을
감지해서 알림을 보내면, 그걸 본 사람이 파일을 고치고 이 DAG를 수동으로
실행한다.

이 DAG가 끝나면 Asset("toll_bronze_updated")을 내보내서
toll_silver_gold_pipeline이 자동으로 이어서 돈다.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

from src.common.alerts import notify_slack_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

TOLL_BRONZE_UPDATED = Asset("toll_bronze_updated")


@dag(
    dag_id="toll_bronze_pipeline",
    description="통행료 요금표/시설목록/CBD 폴리곤 Bronze 업로드 (수동 트리거)",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll"],
)
def toll_bronze_pipeline():

    @task(task_id="upload_rates", outlets=[TOLL_BRONZE_UPDATED])
    def upload_rates_task():
        from src.toll.bronze import upload_rates
        return str(upload_rates())

    @task(task_id="upload_facilities")
    def upload_facilities_task():
        from src.toll.bronze import upload_facilities
        return str(upload_facilities())

    @task(task_id="upload_cbd_geofence")
    def upload_cbd_geofence_task():
        from src.toll.bronze import upload_cbd_geofence
        return str(upload_cbd_geofence())

    upload_rates_task()
    upload_facilities_task()
    upload_cbd_geofence_task()


toll_bronze_pipeline()
```

- [x] **Step 2: dags/toll_silver_gold_pipeline.py 작성**

```python
"""
DAG: toll_silver_gold_pipeline

toll_bronze_pipeline이 요금표/시설목록/CBD 폴리곤을 갱신할 때마다
(Asset("toll_bronze_updated") 트리거) Silver2 매핑을 다시 만들고 Gold
값을 재계산해서 DynamoDB에 적재한다. 요금표가 1년에 한 번 정도만
바뀌므로 cron 스케줄 없이 Asset 트리거만 쓴다(gold_closure_penalty와
동일한 패턴).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

from src.common.alerts import notify_slack_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}


@dag(
    dag_id="toll_silver_gold_pipeline",
    description="통행료 Silver2 매핑 + Gold 계산 (toll_bronze_pipeline Asset 트리거)",
    schedule=[Asset("toll_bronze_updated")],
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "asset-triggered"],
)
def toll_silver_gold_pipeline():

    @task(task_id="build_facility_mapping")
    def build_facility_mapping():
        from pathlib import Path

        from src.common.config import BRONZE_DIR
        from src.toll.silver2 import build_lion_facility_mapping

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return build_lion_facility_mapping(gdb_path=Path(gdb_candidates[-1]))

    @task(task_id="build_zone_mapping")
    def build_zone_mapping():
        from pathlib import Path

        from src.common.config import BRONZE_DIR
        from src.toll.silver2 import build_lion_cbd_mapping

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return build_lion_cbd_mapping(gdb_path=Path(gdb_candidates[-1]))

    @task(task_id="build_and_write_gold")
    def build_and_write_gold(facility_map_path: str, zone_map_path: str):
        from pathlib import Path

        from src.toll.gold import build_and_write

        return build_and_write(
            facility_map_path=Path(facility_map_path),
            zone_map_path=Path(zone_map_path),
        )

    facility_map = build_facility_mapping()
    zone_map = build_zone_mapping()
    build_and_write_gold(facility_map, zone_map)


toll_silver_gold_pipeline()
```

- [x] **Step 3: 두 DAG 모두 smoke import 확인**

```bash
python -c "
import dags.toll_bronze_pipeline
import dags.toll_silver_gold_pipeline
print('OK')
"
```
Expected: `OK` 출력, ImportError 없음. (Airflow가 로컬에 없으면 `docker compose exec airflow-scheduler python -c "..."`로 컨테이너 안에서 실행)

- [x] **Step 4: 커밋**

```bash
git add dags/toll_bronze_pipeline.py dags/toll_silver_gold_pipeline.py
git commit -m "feat: 통행료 Bronze DAG + Asset 트리거 Gold DAG 연결"
```

---

## 완료 후 확인 사항

- [x] `docker compose up -d dynamodb-local` 후 `docker compose exec airflow-scheduler airflow dags trigger toll_bronze_pipeline`로 전체 파이프라인이 끝까지 도는지 수동 확인
  — ~~막힘: ... 별도로 조사 필요~~ **(정정, 원인 확인/해결됨)**: 처음엔 "스케줄러가 고장났다"로 오판했으나 실제 원인은 두 가지였다. (1) Airflow 메타데이터 DB에 이미 지운 도메인(`construction_pipeline` 등)의 실행 기록이 총 2,249건 orphan으로 남아있었음 — `airflow dags delete -y <dag_id>`로 정리(진짜 부채였음, 우리 nav-domain-cleanup 때 DB 정리를 안 해서 생김). (2) 새로 만든 DAG가 **기본적으로 paused 상태**였는데, DAG가 DB에 등록되기 전에 `unpause`를 시도해서 무효였던 걸 "환경 이슈"로 착각함. DAG 등록 후 다시 `airflow dags unpause`하니 정상적으로 전체 파이프라인이 끝까지 성공했다.
  — Bronze/Silver2/Gold 각 함수는 Task 2~6에서 실제 데이터로 개별 검증 완료(문서 내 각 태스크의 "실행 중 발견한 이슈" 참고), 이제 DAG 트리거를 통한 end-to-end 실행도 확인됨.
- [x] `python -c "from src.toll.gold import get_toll_value; print(get_toll_value('아는_다리_세그먼트_id', 5))"`로 실제 값이 나오는지 확인 (Task 6에서 완료 — Lincoln Tunnel segment $16.79, CBD zone segment $0.75)
- [x] Task 2의 CBD Geofence URL, 요금표 금액 두 TODO를 실제 값으로 교체했는지 확인 (완료)

## 사후 리팩터: 이름 정리 (2026-08-22)

DAG 이름과 실제 하는 일이 안 맞는다는 지적으로 아래처럼 이름을 바꿨다:
- `dags/toll_gold_pipeline.py` → `dags/toll_silver_gold_pipeline.py`(`dag_id`도 동일하게 변경) — 이 DAG가 실제로는 Silver2 매핑 + Gold 계산을 둘 다 하는데 "gold"만 붙어있어서 헷갈림
- 매핑 관련 이름에 조인하는 두 주체를 명시: `MAP_TOLL_FACILITY_SEGMENT_PATH`→`MAP_LION_FACILITY_PATH`, `MAP_CBD_ZONE_SEGMENT_PATH`→`MAP_LION_CBD_PATH`, `match_toll_facilities`→`match_lion_facilities`, `build_map_toll_facility_segment`→`build_lion_facility_mapping`, `match_cbd_zone`→`match_lion_cbd`, `build_map_cbd_zone_segment`→`build_lion_cbd_mapping`

이 문서의 위 태스크 본문 코드 블록들은 전부 새 이름으로 갱신했다.

## 사후 수정: 컨테이너 환경변수 + Bronze lineage/트리거 (2026-08-22)

운영 중 실제로 겪은 버그 두 건과, 코드 리뷰 중 지적받아 고친 lineage 문제
두 건을 수정했다.

**1) `EndpointConnectionError`/`NoCredentialsError`**: `docker-compose.yml`의
`x-airflow-env`가 `DYNAMO_LOCAL_ENDPOINT`/`APP_ENV`를 컨테이너에 전달하지
않아서, 컨테이너 안에서는 `.env`의 `APP_ENV=local` 설정이 무시되고 항상
aws 모드(`DYNAMO_LOCAL_ENDPOINT` 기본값 `localhost:8002`, 컨테이너 자기
자신을 가리켜서 연결 안 됨)로 떨어졌다. 두 변수를 `x-airflow-env`에
명시적으로 추가해서 해결. 컨테이너 재생성 후 신규 DAG run으로
`find_latest_lion_gdb → build_lion_facility_mapping/build_lion_cbd_mapping
→ build_and_write_gold` 전체가 success로 끝나는 것까지 확인.

**2) `build_lion_facility_mapping`이 Bronze를 안 거침**: 기본
`facilities_path`가 `config/toll_facilities.yaml`(원본)을 직접 가리켜서,
`toll_bronze_pipeline`이 만든 `data/bronze/toll/toll_facilities.yaml`
사본을 안 쓰고 있었다(옆의 `build_lion_cbd_mapping`은 이미 Bronze 경로가
기본값이라 이 함수만 예외였음). 기본값을
`data/bronze/toll/toll_facilities.yaml`로 수정.

**3) LION 갱신이 toll 파이프라인을 안 깨움**: `lion_pipeline`(분기 자동
cron)이 Asset을 하나도 안 내보내서, `toll_silver_gold_pipeline`은 요금표가
바뀔 때만 재계산됐다 — LION만 갱신되고 요금표는 안 바뀌는 흔한 경우(1년에
한 번 정도만 요금이 바뀜) segment 매핑이 조용히 stale 상태로 남는
문제였다. `lion_pipeline`의 `ingest_lion` 태스크에
`outlets=[Asset("lion_bronze_updated")]` 추가, `toll_silver_gold_pipeline`의
`schedule`을 `Asset("toll_bronze_updated") | Asset("lion_bronze_updated")`로
변경(리스트로 넘기면 AND로 해석돼서 둘 다 갱신돼야 트리거되므로 반드시
`|` 연산자로 OR을 명시해야 함 — 실제로 확인).

## 사후 리팩터: type=4/5 통합 (2026-08-23)

혼잡통행료(type=4)와 도로통행료(type=5)를 원래 별개 타입으로 저장했는데,
한 segment가 CBD zone 안이면서 동시에 다리/터널 시설이기도 한 경우(zone
진입 지점의 다리 segment 등) 두 조건이 서로 독립이라 실제로 겹칠 수
있다는 게 확인됐다 — 이 경우 클라이언트가 "이 segment를 지나는 데 드는
통행료 총액"을 알려면 매번 type 4/5를 둘 다 조회해서 더해야 했다.

"택시가 이 segment를 지나는 데 드는 통행료 총액"이 실제로 필요한 값이므로
`build_gold_items`에서 미리 합산해 `TYPE_TOLL=4` 하나로 합쳤다(`TYPE_ROAD_TOLL=5`
제거) — nav-gold 전체 설계(시간=1/길이=2/수요=3/통행료=4)의 4타입 구성과도
맞다. `get_toll_value(segment_id)`도 `toll_type` 인자를 없앴다.

실제 데이터로 세 케이스 다 확인: zone만 걸린 segment(0.75), 시설만 걸린
segment(7.46), 둘 다 겹치는 segment(0.75+16.79=17.54, 합산 정확히 확인).
