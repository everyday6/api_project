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
- MTA Central Business District Geofence의 정확한 다운로드 URL은 이 플랜 작성 시점에 확인된 공식 파일 링크가 아니라 카탈로그 페이지(`https://catalog.data.gov/dataset/mta-central-business-district-geofence-beginning-june-2024`)만 확인됐다 — **Task 2 착수 전 그 페이지에서 실제 GeoJSON/Shapefile 다운로드 링크를 직접 확인할 것.**
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

- [ ] **Step 1: docker-compose.yml에 dynamodb-local 서비스 추가**

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

- [ ] **Step 2: src/common/config.py에 Dynamo 설정 추가**

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

- [ ] **Step 3: 테스트 폴더 생성**

```bash
mkdir -p tests/common
```

- [ ] **Step 4: 실패하는 테스트 작성**

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

- [ ] **Step 5: 테스트 실패 확인**

먼저 dynamodb-local을 띄운다:
```bash
docker compose up -d dynamodb-local
```

Run: `APP_ENV=local pytest tests/common/test_dynamo.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.common.dynamo'`

- [ ] **Step 6: src/common/dynamo.py 작성**

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

import boto3

from src.common.config import APP_ENV, DYNAMO_LOCAL_ENDPOINT, DYNAMO_REGION, NAV_GOLD_TABLE

_resource = None


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

    get_resource().Table(table_name).put_item(Item=item)


def batch_write_items(items: list[dict], table_name: str = NAV_GOLD_TABLE) -> None:
    """여러 아이템을 배치로 쓴다. boto3의 batch_writer가 25개 단위로
    알아서 나눠 보내고 실패 시 재시도한다."""

    table = get_resource().Table(table_name)
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def get_value(segment_id: str, sk: str, table_name: str = NAV_GOLD_TABLE, default=0):
    """(segment_id, sk) 하나를 조회한다. 없으면 default를 반환한다 —
    nav 골드 데이터셋의 "무결점 응답" 원칙: 값이 없어도 절대 None/에러를
    반환하지 않는다."""

    response = get_resource().Table(table_name).get_item(Key={"segment_id": segment_id, "sk": sk})
    item = response.get("Item")
    return item["value"] if item is not None else default


def batch_get_values(
    segment_ids: list[str],
    sk: str,
    table_name: str = NAV_GOLD_TABLE,
    default=0,
) -> list:
    """segment_id 목록 + 고정 sk로 값 목록을 조회한다. 응답 순서는
    요청한 segment_ids 순서와 항상 동일하다(DynamoDB BatchGetItem 자체는
    순서를 보장 안 해서 직접 맞춰준다). 없는 segment_id는 default로 채운다.
    """

    if not segment_ids:
        return []

    table = get_resource()
    found: dict[str, object] = {}

    # BatchGetItem은 한 번에 최대 100개 키만 허용한다.
    for i in range(0, len(segment_ids), 100):
        chunk = segment_ids[i : i + 100]
        keys = [{"segment_id": sid, "sk": sk} for sid in chunk]
        response = table.meta.client.batch_get_item(
            RequestItems={table_name: {"Keys": keys}}
        )
        for item in response["Responses"][table_name]:
            found[item["segment_id"]] = item["value"]

    return [found.get(sid, default) for sid in segment_ids]
```

- [ ] **Step 7: 테스트 통과 확인**

Run: `APP_ENV=local pytest tests/common/test_dynamo.py -v`
Expected: 4개 테스트 전부 PASS

- [ ] **Step 8: 커밋**

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

- [ ] **Step 1: 폴더/테스트 폴더 생성**

```bash
mkdir -p src/toll tests/toll
touch src/toll/__init__.py tests/toll/__init__.py
```

- [ ] **Step 2: config/toll_rates.yaml 작성**

```yaml
# 택시 전용 통행료 요금표 — 사람이 직접 관리한다(자동 크롤링 없음).
# 공식 요금 페이지가 바뀌면 dags/toll_rate_monitor.py가 Slack으로 알려주고,
# 그때 이 파일을 직접 고친 뒤 dags/toll_bronze_pipeline.py를 수동 트리거한다.
#
# TODO(팀 검토 필요): 아래 금액은 2026-08 기준 조사값이다. 실제 반영 전
# mta.info/fares-tolls/tolls/congestion-relief-zone, panynj.gov 요금표에서
# 재확인할 것.

congestion:
  # 택시/그린캡: zone(60번가 이남) 통과 트립마다 정액, 시간대 무관, 하루 상한 없음.
  taxi_flat_rate: 0.75

road:
  # facility_key는 config/toll_facilities.yaml의 키와 일치해야 한다.
  # 택시 전용 요금이 별도로 없으면 passenger(E-ZPass 승용차) 요금을 그대로 쓴다.
  verrazzano_narrows:
    passenger: 10.67
  queens_midtown_tunnel:
    passenger: 6.94
  hugh_l_carey_tunnel:
    passenger: 6.94
  lincoln_tunnel:
    passenger: 17.00
  holland_tunnel:
    passenger: 17.00
  george_washington_bridge:
    passenger: 17.00
```

- [ ] **Step 3: config/toll_facilities.yaml 작성**

```yaml
# 다리/터널 시설 목록 — LION의 Street 컬럼에서 이 패턴을 포함하는
# segment를 그 시설로 분류한다(대소문자 무시, 부분 일치).
# facility_key는 config/toll_rates.yaml의 road 아래 키와 반드시 일치해야 한다.

verrazzano_narrows:
  street_contains: "VERRAZZANO"
queens_midtown_tunnel:
  street_contains: "QUEENS MIDTOWN TUNNEL"
hugh_l_carey_tunnel:
  street_contains: "HUGH L CAREY"
lincoln_tunnel:
  street_contains: "LINCOLN TUNNEL"
holland_tunnel:
  street_contains: "HOLLAND TUNNEL"
george_washington_bridge:
  street_contains: "GEO WASHINGTON BR"
```

- [ ] **Step 4: 실패하는 테스트 작성**

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

- [ ] **Step 5: 테스트 실패 확인**

Run: `pytest tests/toll/test_bronze.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.bronze'`

- [ ] **Step 6: src/toll/bronze.py 작성**

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

# TODO(팀 검토 필요): 카탈로그 페이지에서 실제 GeoJSON 다운로드 링크로 교체할 것.
CBD_GEOFENCE_URL = "https://data.ny.gov/resource/PLACEHOLDER-VERIFY-BEFORE-USE.geojson"


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

- [ ] **Step 7: 테스트 통과 확인**

Run: `pytest tests/toll/test_bronze.py -v`
Expected: 2개 테스트 전부 PASS

- [ ] **Step 8: 커밋**

```bash
git add config/toll_rates.yaml config/toll_facilities.yaml src/toll/__init__.py src/toll/bronze.py tests/toll/__init__.py tests/toll/test_bronze.py
git commit -m "feat: 통행료 요금표/시설목록/CBD 폴리곤 Bronze 업로드 추가"
```

---

### Task 3: 요금표 변경 감지 + 월간 알람 DAG

**Files:**
- Create: `src/toll/rate_monitor.py`
- Test: `tests/toll/test_rate_monitor.py`
- Create: `dags/toll_rate_monitor.py`

**Interfaces:**
- Consumes: `src.common.alerts.notify_slack_message`
- Produces: `src.toll.rate_monitor.{RATE_PAGE_URLS, check_pages_changed}` (dags/toll_rate_monitor.py가 매달 이 함수를 호출)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/toll/test_rate_monitor.py`:
```python
from unittest.mock import patch

from src.toll.rate_monitor import check_pages_changed


def test_check_pages_changed_detects_no_change_on_first_run(tmp_path):
    state_path = tmp_path / "last_hash.json"

    with patch("src.toll.rate_monitor._fetch_page_text", return_value="rate table content v1"):
        changed = check_pages_changed(urls=["https://example.com/tolls"], state_path=state_path)

    # 첫 실행은 "이전 상태"가 없어서 비교 대상이 없으므로 변경 없음으로 처리.
    assert changed == []
    assert state_path.exists()


def test_check_pages_changed_detects_change(tmp_path):
    state_path = tmp_path / "last_hash.json"
    url = "https://example.com/tolls"

    with patch("src.toll.rate_monitor._fetch_page_text", return_value="rate table content v1"):
        check_pages_changed(urls=[url], state_path=state_path)

    with patch("src.toll.rate_monitor._fetch_page_text", return_value="rate table content v2 (changed!)"):
        changed = check_pages_changed(urls=[url], state_path=state_path)

    assert changed == [url]


def test_check_pages_changed_no_change_when_content_same(tmp_path):
    state_path = tmp_path / "last_hash.json"
    url = "https://example.com/tolls"

    with patch("src.toll.rate_monitor._fetch_page_text", return_value="same content"):
        check_pages_changed(urls=[url], state_path=state_path)
        changed = check_pages_changed(urls=[url], state_path=state_path)

    assert changed == []
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_rate_monitor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.rate_monitor'`

- [ ] **Step 3: src/toll/rate_monitor.py 작성**

```python
"""
통행료 공식 요금 페이지 변경 감지

값을 자동으로 파싱/반영하지 않는다(docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md
참고 — 요금 페이지는 구조가 안정된 API가 아니라 사람이 확인해야 하는
웹페이지라, 잘못 파싱해도 조용히 틀린 값이 들어갈 위험이 있다). 이
모듈은 페이지 텍스트의 해시값만 이전 실행과 비교해서 "바뀐 것 같다"만
판단하고, 실제 값 반영은 사람이 config/toll_rates.yaml을 직접 고치게 한다.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import requests

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_rate_monitor")

# TODO(팀 검토 필요): 실제 모니터링할 공식 요금 페이지 URL로 채울 것.
RATE_PAGE_URLS = [
    "https://www.mta.info/fares-tolls/tolls/vehicle-types",
    "https://www.mta.info/fares-tolls/tolls/congestion-relief-zone",
]


def _fetch_page_text(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def check_pages_changed(urls: list[str] = RATE_PAGE_URLS, state_path: Path = Path("data/tmp/toll_rate_hashes.json")) -> list[str]:
    """각 url의 페이지 내용을 해시로 이전 실행과 비교한다.
    처음 보는 url이면(이전 기록 없음) 비교 대상이 없으므로 변경 없음으로
    취급하고 해시만 기록한다. 반환값은 실제로 바뀐 url 목록."""

    state_path.parent.mkdir(parents=True, exist_ok=True)
    previous: dict[str, str] = {}
    if state_path.exists():
        previous = json.loads(state_path.read_text())

    changed: list[str] = []
    current: dict[str, str] = dict(previous)

    for url in urls:
        new_hash = _hash(_fetch_page_text(url))
        old_hash = previous.get(url)
        if old_hash is not None and old_hash != new_hash:
            changed.append(url)
            logger.warning(f"[toll_rate_monitor] 페이지 변경 감지: {url}")
        current[url] = new_hash

    state_path.write_text(json.dumps(current))
    return changed
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_rate_monitor.py -v`
Expected: 3개 테스트 전부 PASS

- [ ] **Step 5: dags/toll_rate_monitor.py 작성**

```python
"""
DAG: toll_rate_monitor

통행료 공식 요금 페이지가 바뀌었는지 매달 확인하고, 바뀌었으면 Slack으로
알린다. 값을 자동으로 반영하지 않는다 — 알림을 받은 사람이
config/toll_rates.yaml을 직접 확인/수정한 뒤 toll_bronze_pipeline DAG를
수동 트리거해야 한다(src/toll/rate_monitor.py 모듈 docstring 참고).
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
    description="통행료 공식 요금 페이지 변경 감지 (월 1회)",
    schedule="0 9 1 * *",  # 매달 1일 오전 9시
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "monthly"],
)
def toll_rate_monitor():

    @task(task_id="check_and_alert")
    def check_and_alert():
        from src.toll.rate_monitor import check_pages_changed

        changed = check_pages_changed()
        if changed:
            urls_text = "\n".join(f"- {url}" for url in changed)
            notify_slack_message(
                f":warning: 통행료 요금 페이지 변경 감지\n{urls_text}\n"
                f"config/toll_rates.yaml 확인 후 toll_bronze_pipeline DAG를 수동 트리거하세요."
            )

    check_and_alert()


toll_rate_monitor()
```

- [ ] **Step 6: 커밋**

```bash
git add src/toll/rate_monitor.py tests/toll/test_rate_monitor.py dags/toll_rate_monitor.py
git commit -m "feat: 통행료 요금 페이지 변경 감지 + 월간 Slack 알람 DAG 추가"
```

---

### Task 4: Silver2 — LION segment 추출 + 다리/터널 시설 매칭

**Files:**
- Create: `src/toll/silver2.py`
- Test: `tests/toll/test_silver2.py`

**Interfaces:**
- Consumes: `src.toll.bronze.BRONZE_ROOT`(경로 규칙), LION Bronze GDB(`data/bronze/lion/version_date=*/lion/lion.gdb`)
- Produces: `src.toll.silver2.{MAP_TOLL_FACILITY_SEGMENT_PATH, load_lion_segments, match_toll_facilities, build_map_toll_facility_segment}` (Task 6이 이 매핑을 읽음)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/toll/test_silver2.py`:
```python
import geopandas as gpd
import pandas as pd
import yaml
from shapely.geometry import LineString

from src.toll.silver2 import match_toll_facilities


def test_match_toll_facilities_matches_by_street_substring(tmp_path):
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

    result = match_toll_facilities(segments, facilities_path)

    assert set(result["segment_id"]) == {"S1", "S3"}
    row_s1 = result[result["segment_id"] == "S1"].iloc[0]
    assert row_s1["facility_key"] == "lincoln_tunnel"
    row_s3 = result[result["segment_id"] == "S3"].iloc[0]
    assert row_s3["facility_key"] == "queens_midtown_tunnel"


def test_match_toll_facilities_excludes_non_matching_segments(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1"],
        "street": ["5 AVENUE"],
        "geometry": [LineString([(0, 0), (1, 1)])],
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({"lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"}}))

    result = match_toll_facilities(segments, facilities_path)

    assert result.empty
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.silver2'`

- [ ] **Step 3: src/toll/silver2.py 작성 (이번 태스크 분량만)**

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

MAP_TOLL_FACILITY_SEGMENT_PATH = SILVER2_DIR / "map_toll_facility_segment.parquet"


def load_lion_segments(gdb_path: Path) -> gpd.GeoDataFrame:
    """LION Bronze GDB에서 segment_id/street/geometry만 뽑는다."""

    gdf = gpd.read_file(gdb_path, layer="lion")
    gdf = gdf.rename(columns={"SegmentID": "segment_id", "Street": "street"})
    return gdf[["segment_id", "street", "geometry"]]


def match_toll_facilities(segments: gpd.GeoDataFrame, facilities_path: Path) -> pd.DataFrame:
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


def build_map_toll_facility_segment(
    gdb_path: Path,
    facilities_path: Path = Path("config/toll_facilities.yaml"),
    out_path: Path = MAP_TOLL_FACILITY_SEGMENT_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    result = match_toll_facilities(segments, facilities_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] 시설 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: 2개 테스트 전부 PASS

- [ ] **Step 5: 커밋**

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
- Produces: `src.toll.silver2.{MAP_CBD_ZONE_SEGMENT_PATH, match_cbd_zone, build_map_cbd_zone_segment}` (Task 6이 이 매핑을 읽음)

- [ ] **Step 1: 실패하는 테스트 추가**

`tests/toll/test_silver2.py` 끝에 추가:
```python
from shapely.geometry import Polygon

from src.toll.silver2 import match_cbd_zone


def test_match_cbd_zone_keeps_segments_inside_polygon():
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

    result = match_cbd_zone(segments, zone_polygon)

    assert list(result["segment_id"]) == ["INSIDE"]


def test_match_cbd_zone_keeps_segments_touching_boundary():
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}
    )
    # 경계선에 걸치는 segment(zone 진입 지점)도 포함돼야 한다.
    segments = gpd.GeoDataFrame({
        "segment_id": ["BOUNDARY"],
        "geometry": [LineString([(10, 5), (15, 5)])],
    })

    result = match_cbd_zone(segments, zone_polygon)

    assert list(result["segment_id"]) == ["BOUNDARY"]
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_silver2.py -v -k cbd_zone`
Expected: FAIL with `ImportError: cannot import name 'match_cbd_zone'`

- [ ] **Step 3: src/toll/silver2.py에 함수 추가**

`src/toll/silver2.py` 끝에 추가(import문은 파일 상단에 이미 있는 `gpd`/`pd`/`Path`/`SILVER2_DIR`/`logger` 재사용):

```python
MAP_CBD_ZONE_SEGMENT_PATH = SILVER2_DIR / "map_cbd_zone_segment.parquet"


def match_cbd_zone(segments: gpd.GeoDataFrame, zone_polygon: gpd.GeoDataFrame) -> pd.DataFrame:
    """segments 중 CBD(Congestion Relief Zone) 폴리곤과 교차하는(경계에
    걸친 것 포함) segment_id만 반환한다. intersects를 쓰는 이유: zone
    "안"으로 완전히 들어간 segment뿐 아니라 zone 경계를 지나는 진입
    segment도 혼잡통행료 대상이기 때문이다(둘을 구분할 필요 없음 — 스펙
    참고: zone 내부 segment 전부에 값을 넣고 dedup은 클라이언트가 함)."""

    if segments.crs is None:
        segments = segments.set_crs(zone_polygon.crs, allow_override=True)

    joined = gpd.sjoin(segments, zone_polygon, how="inner", predicate="intersects")
    return joined[["segment_id"]].drop_duplicates().reset_index(drop=True)


def build_map_cbd_zone_segment(
    gdb_path: Path,
    cbd_geofence_path: Path = Path("data/bronze/toll/cbd_geofence.geojson"),
    out_path: Path = MAP_CBD_ZONE_SEGMENT_PATH,
) -> str:
    segments = load_lion_segments(gdb_path)
    zone_polygon = gpd.read_file(cbd_geofence_path)

    result = match_cbd_zone(segments, zone_polygon)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(str(out_path), index=False)

    logger.info(f"[toll_silver2] CBD zone 매핑 {len(result)}행 저장 -> {out_path}")
    return str(out_path)
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_silver2.py -v`
Expected: 4개 테스트 전부 PASS

- [ ] **Step 5: 커밋**

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
- Consumes: `src.toll.silver2.{MAP_TOLL_FACILITY_SEGMENT_PATH, MAP_CBD_ZONE_SEGMENT_PATH}`, `src.common.dynamo.{batch_write_items, get_value}`
- Produces: `src.toll.gold.{TYPE_CONGESTION, TYPE_ROAD_TOLL, load_rate_table, build_gold_items, write_gold_items, get_toll_value}` (서빙 API가 `get_toll_value`를 호출)

- [ ] **Step 1: 실패하는 테스트 작성**

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

- [ ] **Step 2: 테스트 실패 확인**

Run: `pytest tests/toll/test_gold.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.toll.gold'`

- [ ] **Step 3: src/toll/gold.py 작성**

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
from src.toll.silver2 import MAP_CBD_ZONE_SEGMENT_PATH, MAP_TOLL_FACILITY_SEGMENT_PATH

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

    return items


def write_gold_items(items: list[dict]) -> None:
    dynamo.ensure_table()
    dynamo.batch_write_items(items)
    logger.info(f"[toll_gold] DynamoDB에 {len(items)}개 아이템 적재 완료")


def build_and_write(
    rate_table_path: Path = Path("data/bronze/toll/toll_rates.yaml"),
    zone_map_path: Path = MAP_CBD_ZONE_SEGMENT_PATH,
    facility_map_path: Path = MAP_TOLL_FACILITY_SEGMENT_PATH,
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

- [ ] **Step 4: 테스트 통과 확인**

Run: `pytest tests/toll/test_gold.py -v`
Expected: 4개 테스트 전부 PASS

- [ ] **Step 5: get_toll_value 테스트 추가 및 확인**

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

- [ ] **Step 6: 커밋**

```bash
git add src/toll/gold.py tests/toll/test_gold.py
git commit -m "feat: 통행료 Gold 계산 + DynamoDB 적재 + 서빙 조회 함수(get_toll_value) 추가"
```

---

### Task 7: Airflow DAG 연결 (Bronze DAG + Asset 트리거 Gold DAG)

**Files:**
- Create: `dags/toll_bronze_pipeline.py`
- Create: `dags/toll_gold_pipeline.py`

**Interfaces:**
- Consumes: `src.toll.bronze.{upload_rates, upload_facilities, upload_cbd_geofence}`, `src.toll.silver2.{build_map_toll_facility_segment, build_map_cbd_zone_segment}`, `src.toll.gold.build_and_write`

- [ ] **Step 1: dags/toll_bronze_pipeline.py 작성**

```python
"""
DAG: toll_bronze_pipeline

통행료 요금표/시설목록/CBD 폴리곤을 Bronze에 올린다. 요금표는 사람이
config/toll_rates.yaml을 고친 뒤에만 값이 바뀌므로 cron 스케줄이 아니라
수동 트리거(schedule=None)로 둔다 — toll_rate_monitor DAG가 변경을
감지해서 알림을 보내면, 그걸 본 사람이 파일을 고치고 이 DAG를 수동으로
실행한다.

이 DAG가 끝나면 Asset("toll_bronze_updated")을 내보내서
toll_gold_pipeline이 자동으로 이어서 돈다.
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

- [ ] **Step 2: dags/toll_gold_pipeline.py 작성**

```python
"""
DAG: toll_gold_pipeline

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
    dag_id="toll_gold_pipeline",
    description="통행료 Silver2 매핑 + Gold 계산 (toll_bronze_pipeline Asset 트리거)",
    schedule=[Asset("toll_bronze_updated")],
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "asset-triggered"],
)
def toll_gold_pipeline():

    @task(task_id="build_facility_mapping")
    def build_facility_mapping():
        from pathlib import Path

        from src.common.config import BRONZE_DIR
        from src.toll.silver2 import build_map_toll_facility_segment

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return build_map_toll_facility_segment(gdb_path=Path(gdb_candidates[-1]))

    @task(task_id="build_zone_mapping")
    def build_zone_mapping():
        from pathlib import Path

        from src.common.config import BRONZE_DIR
        from src.toll.silver2 import build_map_cbd_zone_segment

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return build_map_cbd_zone_segment(gdb_path=Path(gdb_candidates[-1]))

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


toll_gold_pipeline()
```

- [ ] **Step 3: 두 DAG 모두 smoke import 확인**

```bash
python -c "
import dags.toll_bronze_pipeline
import dags.toll_gold_pipeline
print('OK')
"
```
Expected: `OK` 출력, ImportError 없음. (Airflow가 로컬에 없으면 `docker compose exec airflow-scheduler python -c "..."`로 컨테이너 안에서 실행)

- [ ] **Step 4: 커밋**

```bash
git add dags/toll_bronze_pipeline.py dags/toll_gold_pipeline.py
git commit -m "feat: 통행료 Bronze DAG + Asset 트리거 Gold DAG 연결"
```

---

## 완료 후 확인 사항

- [ ] `docker compose up -d dynamodb-local` 후 `docker compose exec airflow-scheduler airflow dags trigger toll_bronze_pipeline`로 전체 파이프라인이 끝까지 도는지 수동 확인
- [ ] `python -c "from src.toll.gold import get_toll_value; print(get_toll_value('아는_다리_세그먼트_id', 5))"`로 실제 값이 나오는지 확인
- [ ] Task 2의 CBD Geofence URL, 요금표 금액 두 TODO를 실제 값으로 교체했는지 확인
