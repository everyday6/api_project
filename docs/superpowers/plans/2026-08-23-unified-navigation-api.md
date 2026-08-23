# 통합 내비게이션 API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `nav_api.py`(Lambda)에 새 엔드포인트 `POST /api/navigation/values`를 추가해서, type1(소요시간)/type2(길이)/type3(TLC 수요)를 하나의 요청/응답 형식으로 조회할 수 있게 한다. 각 팀이 이미 만든 조회 로직(`resolve_segment_values`, `get_type3_values`)을 재사용하고, EC2에서 uvicorn으로 돌던 팀원의 `navigation-api` 서비스를 제거한다.

**Architecture:** 기존 `src/serving/nav_api.py`(FastAPI, Lambda로 배포됨)에 새 Pydantic 모델과 라우트를 추가한다. `type` 값으로 내부에서 분기해서, type1/2는 기존 `resolve_segment_values()`를, type3는 `src/serving/api.py`의 `get_type3_values()`를 그대로 import해서 호출한다. 새 로직을 만들지 않는다 — 기존 함수를 다른 요청/응답 포맷으로 감싸기만 한다.

**Tech Stack:** FastAPI, Pydantic, 기존 `nav_lookup.py`/`serving/api.py`

## Global Constraints

- 새 엔드포인트: `POST /api/navigation/values`. 기존 `/segments/values`는 그대로 유지(삭제 금지) — 다른 소비자가 쓰고 있을 수 있음.
- 요청: `{"segment_ids": [...], "type": 1|2|3, "date": "YYYY-MM-DD", "time": "HH:MM"}`
- 응답: `{"value": [float, ...]}` — 항상 float, segment_ids와 같은 순서.
- `date`는 type1/2에서는 안 쓰고(무시), type3에서만 `time`과 합쳐 `datetime`으로 만들어 씀.
- `type`은 `Literal[1, 2, 3]`만 허용 — `4` 등 다른 값은 FastAPI가 자동으로 422를 반환하게 둔다(별도 처리 코드 불필요).
- `segment_ids`는 1개 이상 500개 이하(기존 `/segments/values`와 동일한 상한 — type1 순차조회 특성상 상한 필요).
- `src/serving/api.py`, `src/serving/nav_lookup.py`는 수정하지 않는다 — import만 해서 재사용.

## File Structure

- Modify: `src/serving/nav_api.py` — `NavigationValuesRequest`/`NavigationValuesResponse` 모델, dispatch 함수, `POST /api/navigation/values` 라우트 추가
- Test: `tests/serving/test_nav_api.py` — 신규 테스트 추가
- Modify: `docker-compose.yml` — `navigation-api` 서비스 정의 및 `WATCH_CONTAINERS`의 `navigation-api` 항목 제거

---

### Task 1: 통합 엔드포인트 추가 (TDD)

**Files:**
- Modify: `src/serving/nav_api.py`
- Test: `tests/serving/test_nav_api.py`

**Interfaces:**
- Consumes: `src.serving.nav_lookup.resolve_segment_values(segment_ids, type, time) -> list[int]` (기존, 시그니처 그대로), `src.serving.api.get_type3_values(segment_ids, requested_at: datetime) -> list[float]` (기존, 시그니처 그대로)
- Produces: `POST /api/navigation/values` — 요청 `{"segment_ids": [...], "type": 1|2|3, "date": "YYYY-MM-DD", "time": "HH:MM"}` → 응답 `{"value": [float, ...]}`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/serving/test_nav_api.py` 끝에 추가:

```python
def test_navigation_values_type1_dispatches_to_resolve_segment_values():
    with patch("src.serving.nav_api.resolve_segment_values", return_value=[30, 50]) as mock_resolve:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1", "2"], "type": 1, "date": "2026-08-23", "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {"value": [30.0, 50.0]}
    mock_resolve.assert_called_once_with(["1", "2"], 1, "12:00")


def test_navigation_values_type2_dispatches_to_resolve_segment_values():
    with patch("src.serving.nav_api.resolve_segment_values", return_value=[100]) as mock_resolve:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1"], "type": 2, "date": "2026-08-23", "time": "12:00"},
        )

    assert response.status_code == 200
    assert response.json() == {"value": [100.0]}
    mock_resolve.assert_called_once_with(["1"], 2, "12:00")


def test_navigation_values_type3_combines_date_and_time_into_datetime():
    from datetime import datetime

    with patch("src.serving.nav_api.get_type3_values", return_value=[12.5, 7.0]) as mock_type3:
        response = client.post(
            "/api/navigation/values",
            json={"segment_ids": ["1", "2"], "type": 3, "date": "2026-08-23", "time": "14:30"},
        )

    assert response.status_code == 200
    assert response.json() == {"value": [12.5, 7.0]}
    mock_type3.assert_called_once_with(["1", "2"], datetime(2026, 8, 23, 14, 30))


def test_navigation_values_rejects_type4():
    response = client.post(
        "/api/navigation/values",
        json={"segment_ids": ["1"], "type": 4, "date": "2026-08-23", "time": "12:00"},
    )

    assert response.status_code == 422


def test_navigation_values_rejects_malformed_date():
    response = client.post(
        "/api/navigation/values",
        json={"segment_ids": ["1"], "type": 1, "date": "2026/08/23", "time": "12:00"},
    )

    assert response.status_code == 422


def test_navigation_values_rejects_too_many_segment_ids():
    response = client.post(
        "/api/navigation/values",
        json={
            "segment_ids": [str(i) for i in range(501)],
            "type": 1,
            "date": "2026-08-23",
            "time": "12:00",
        },
    )

    assert response.status_code == 422
```

- [ ] **Step 2: 실패 확인**

```bash
.venv/bin/pytest tests/serving/test_nav_api.py -v -k navigation_values
```

Expected: 전부 FAIL — `/api/navigation/values` 라우트가 없어서 404, `get_type3_values`/`resolve_segment_values`가 `src.serving.nav_api`에 없어서 patch 대상 못 찾음(AttributeError).

- [ ] **Step 3: 최소 구현**

`src/serving/nav_api.py`에서 기존 `from src.serving.nav_lookup import resolve_segment_values` 아래에 추가:

```python
from datetime import datetime

from src.serving.api import get_type3_values
```

기존 `SegmentValuesResponse` 클래스 뒤, `@app.exception_handler` 앞에 추가:

```python
class NavigationValuesRequest(BaseModel):
    segment_ids: list[str] = Field(min_length=1, max_length=500)
    type: Literal[1, 2, 3]
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")


class NavigationValuesResponse(BaseModel):
    value: list[float]


def _resolve_navigation_values(
    segment_ids: list[str], type_: int, date: str, time: str
) -> list[float]:
    if type_ == 3:
        requested_at = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
        return get_type3_values(segment_ids, requested_at)
    return [float(v) for v in resolve_segment_values(segment_ids, type_, time)]
```

파일 맨 끝(`get_segment_values` 함수 뒤)에 추가:

```python
@app.post("/api/navigation/values", response_model=NavigationValuesResponse)
def get_navigation_values(request: NavigationValuesRequest) -> NavigationValuesResponse:
    values = _resolve_navigation_values(
        request.segment_ids, request.type, request.date, request.time
    )
    return NavigationValuesResponse(value=values)
```

- [ ] **Step 4: 통과 확인**

```bash
.venv/bin/pytest tests/serving/test_nav_api.py -v
```

Expected: 전체 PASS (기존 테스트 포함, 신규 6개 포함).

- [ ] **Step 5: 커밋**

```bash
git add src/serving/nav_api.py tests/serving/test_nav_api.py
git commit -m "feat: type1/2/3 통합 조회 엔드포인트(/api/navigation/values) 추가"
```

---

### Task 2: EC2 navigation-api 서비스 제거

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: 없음
- Produces: 없음 (인프라 정리)

- [ ] **Step 1: `navigation-api` 서비스 블록 제거**

`docker-compose.yml`에서 `navigation-api:` 로 시작하는 서비스 정의 전체(`container_name: navigation-api`, `uvicorn src.serving.api:app --host 0.0.0.0 --port 8000` 커맨드 포함)를 삭제한다.

- [ ] **Step 2: `WATCH_CONTAINERS`에서 이름 제거**

`WATCH_CONTAINERS` 환경변수 값에서 `navigation-api` 항목을 제거한다 (쉼표로 구분된 목록 중 하나).

- [ ] **Step 3: docker-compose 문법 검증**

```bash
docker compose config --quiet
```

Expected: 에러 없이 종료(exit 0).

- [ ] **Step 4: 커밋**

```bash
git add docker-compose.yml
git commit -m "chore: EC2 navigation-api 서비스 제거 (type3 로직이 Lambda로 통합됨, 팀원 확인 완료)"
```

---

## Self-Review 메모 (플랜 작성자용, 실행 시 무시)

- Task 1의 `_resolve_navigation_values`는 `type_ in (1, 2)`에 대한 명시적 분기 없이 else 경로로 처리한다 — `Literal[1,2,3]`가 Pydantic 단에서 이미 3가지 값만 허용하므로, type3 분기를 벗어나면 남는 건 1/2뿐이라 안전하다. 방어적인 `else: raise` 코드는 도달 불가능해서 추가하지 않는다.
- `src/serving/api.py`와 `src/serving/nav_lookup.py`는 이 플랜에서 수정하지 않는다 — 순수 재사용.
