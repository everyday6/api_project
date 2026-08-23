# 통합 내비게이션 API Design

**Goal:** 각 팀이 따로 만든 세그먼트 지표 서빙 API(type1/2는 우리 Lambda, type3는 팀원 EC2 서비스, type4/5는 서빙 API 자체가 없음)를 하나의 엔드포인트로 통합한다. 내비게이션 엔진이 경로를 계산할 때, 타입 하나만 알면 항상 일관된 형식으로 값을 조회할 수 있어야 한다.

**Architecture:** 기존 `src/serving/nav_api.py`(현재 Lambda로 배포된 FastAPI 앱)를 확장해서 새 엔드포인트 `POST /api/navigation/values`를 추가한다. `type` 값으로 내부 분기해서 각 팀이 이미 만들어둔 조회 로직을 그대로 재사용한다 — 새로 재작성하지 않는다. EC2에서 uvicorn으로 돌던 팀원의 `src/serving/api.py`(`navigation-api` 서비스)는 이 작업 완료 후 폐기 대상이다 — "무조건 응답" 원칙(EC2 단일 인스턴스 장애 시에도 응답)을 지키려면 서빙은 전부 Lambda여야 한다.

**Tech Stack:** 기존 FastAPI(`nav_api.py`) + Mangum + Lambda, 기존 DynamoDB 조회 함수 재사용(`nav_lookup.py`, `serving/api.py`의 `get_type3_values`, `toll/gold.py`의 `get_toll_value`)

## Global Constraints

- 새 엔드포인트는 `POST /api/navigation/values`. 기존 `/segments/values`는 하위 호환을 위해 그대로 둔다(삭제하지 않음) — 다른 소비자가 이미 쓰고 있을 수 있음.
- 응답 값은 항상 `float` — type1/2는 현재 `int`를 반환하므로 캐스팅 필요.
- `date` 필드는 요청에 받되, type1/2/4 로직에서는 쓰지 않는다(현재 데이터 모델에 날짜/요일 차원이 없음). type3에서만 실제로 사용한다.
- 응답 순서는 요청 `segment_ids` 순서와 동일해야 한다(type1의 누적 소요시간 의미가 순서에 의존함).
- 각 타입의 DynamoDB 테이블/조회 로직은 그대로 둔다(마이그레이션 없음) — API 레이어에서만 통합.
- toll 모듈(`src/common/dynamo.py`)이 쓰는 패키지는 `boto3`뿐이라 Lambda 이미지(`requirements-lambda.txt`)에 이미 포함되어 있음 — 추가 설치 불필요.

## 요청/응답 계약

```
POST /api/navigation/values
{
  "segment_ids": ["1001", "1002", "1003"],
  "type": 1,
  "date": "2026-08-23",
  "time": "12:00"
}

→ 200 OK
{"value": [30.2, 25.0, 18.7]}
```

- `segment_ids`: 1개 이상, 순서 있는 목록
- `type`: `1`(소요시간) | `2`(길이) | `3`(TLC 수요) | `4`(통행료)
- `date`: `YYYY-MM-DD` 문자열
- `time`: `HH:MM` 문자열

## 타입별 디스패치

### type=1 (소요시간), type=2 (길이)

기존 `src/serving/nav_lookup.py`의 `resolve_segment_values(segment_ids, type, time)`를 그대로 호출한다. `date`는 무시. 반환값(`int`)을 `float`로 캐스팅해서 응답.

### type=3 (TLC 수요)

`date` + `time`을 합쳐 `datetime` 객체를 만들고, 기존 `src/serving/api.py`의 `get_type3_values(segment_ids, requested_at)`를 그대로 호출한다.

**전제조건 (이 설계 범위 밖):** `DYNAMODB_NAV_TABLE` 환경변수/테이블이 아직 EC2에 세팅 안 되어 있다 — 팀원이 별도로 만들어야 한다. 테이블이 없는 동안은 `get_type3_values` 내부의 기존 예외 처리 경로(캐시 또는 `0.0` 반환)가 그대로 동작해서 "무조건 응답" 원칙은 깨지지 않는다.

### type=4 (통행료 — 혼잡+도로 합산)

세그먼트마다 `src/toll/gold.py`의 `get_toll_value(segment_id, TYPE_CONGESTION)`과 `get_toll_value(segment_id, TYPE_ROAD_TOLL)`를 각각 조회해서 **더한 값**을 반환한다.

- 혼잡통행료만 있으면 → 그 값
- 도로통행료만 있으면 → 그 값
- 둘 다 있으면 → 합산 (같은 세그먼트가 혼잡구역 경계의 다리/터널일 수 있어서, 실제로 둘 다 부과되는 경우가 있음 — 합치지 않으면 총 경로 비용이 실제보다 싸게 계산됨)
- 둘 다 없으면 → 0 (`get_toll_value`의 기존 기본값)

외부에는 `type=5`(도로통행료 단독)를 별도로 노출하지 않는다 — `type=4` 하나로 흡수.

## 알려진 리스크 / 후속 과제 (이 설계 범위 밖)

- type=4 응답은 합산된 값만 나가서 "혼잡료 얼마 + 통행료 얼마"인지 세부 내역을 잃는다. 나중에 사용자에게 비용 항목을 나눠 보여줄 필요가 생기면, 응답 스키마에 breakdown 필드 추가를 검토한다.
- EC2의 `navigation-api`(uvicorn) 서비스를 언제/어떻게 내릴지는 팀원과 조율이 필요하다 — 이 설계에서는 폐기 "대상"이라고만 명시하고, 실제 제거는 별도 작업으로 다룬다.
- type3용 `DYNAMODB_NAV_TABLE` 테이블 생성은 팀원 담당 — 이 설계는 테이블이 없어도 안전하게 fallback되는 것까지만 보장한다.
