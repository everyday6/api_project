# 브론즈-실버-골드 레이어 역할 통일 리팩토링 설계

## 배경

현재 8개 도메인(construction, construction_stipulations, event, lion, road_closures,
ticketmaster, tlc, taxi_zone)이 각자 Bronze/Silver/Gold 레이어를 쓰고 있지만, "어떤 종류의
처리를 어느 레이어에서 하는가"에 대한 공통 규칙이 없어 도메인마다 제각각이었다. 감사 결과
확인된 대표적인 불일치:

- Manhattan(지역) 필터: construction/tlc는 Gold, event/ticketmaster는 Silver
- "지금 유효한가" 시점 필터: event/ticketmaster는 Silver에서 즉시 드롭, construction은
  아예 필터하지 않고 scoring 단계로 위임
- 중복 제거 정책: construction/event는 예외로 차단, lion/ticketmaster는 조용히 drop,
  road_closures는 비즈니스 규칙 기반 conflation
- `lion/traffic_score.py`, `scoring/closure_penalty.py`처럼 로직은 명백히 Gold(점수 계산)인데
  물리적으로 `SILVER_DIR`에 저장되는 경우
- cross-domain 참조가 서로 다른 레이어를 가리킴 (construction_stipulations는 construction
  **Gold**를 읽는데, road_closures는 construction_stipulations **Silver**를 읽음)

이 설계는 이런 불일치를 없애기 위해 레이어를 5단계로 재정의하고, 8개 도메인 전체를 그
구조로 재배치하는 계획을 다룬다.

## 목표

모든 도메인이 "이런 종류의 처리는 반드시 이 레이어에서 한다"는 하나의 공통 규칙을 따르게
한다. 레이어의 *의미*는 고정하되, 모든 도메인이 5개 레이어를 다 가질 필요는 없다 — 해당
종류의 처리가 없으면 그 레이어를 건너뛴다.

## 범위

**포함**
- 5단계 레이어 정의 및 판별 규칙
- `src/`, `data/` 폴더 구조 재설계
- 8개 도메인 전체의 기존 로직을 새 레이어로 재배치하는 매핑
- `mapping/` → `silver2/`, `scoring/`(api.py 제외) → `gold2/` 이름 변경
- `scoring/api.py` → `serving/api.py` 이동

**제외 (후속 작업)**
- `ticketmaster/venue.py`의 capacity 가중치를 `event_boost.py`에 실제로 연결하는 것 —
  이번 리팩토링은 파일을 올바른 레이어(Silver2)로 옮기기만 하고, 아직 아무도 호출하지 않는
  상태 그대로 유지한다. 실제로 연결하면 스코어링 결과값이 바뀌는 **동작 변경**이라 별도
  작업으로 분리한다.
- `taxi_zone/get_manhattan_zone_ids()` 고아 헬퍼의 삭제 여부 — 구현 단계에서 실제 사용처
  재확인 후 결정한다.
- Airflow DAG의 task 이름을 레이어 표기(`build_silver1_*` 등)에 맞게 전부 바꾸는 세부 작업 —
  구조는 이 설계를 따르되, 정확한 task id 목록은 구현 계획 단계에서 다룬다.
- tlc의 결측치 정책(드롭 대신 로그만 남김)은 집계 편향 방지를 위한 **의도된 예외**로 보고
  정책 자체는 바꾸지 않는다. 다른 도메인과 통일하는 것은 레이어 *위치*(Silver1)뿐이다.

## 레이어 정의

| 레이어 | 역할 | 판별 규칙 |
|---|---|---|
| Bronze | 원본 그대로 저장 | 변환 없음 |
| Silver1 | 단일 소스 정제 | 결측치 제거, 중복 제거(키 기반), 컬럼 프루닝, 이름/타입 표준화, 구조 분해(날짜+시간 분리 등). 이미 있는 정보의 **표현 형식**만 바꾸고 새 파생 컬럼을 만들지 않음 |
| Silver2 | 조인 결과물 | 여러 소스를 구조적으로 연결(공간조인, 그래프 매칭, 엔티티 conflation). 필터링이나 새 수치 계산 없이 "연결된 원자료"만. **전 지역을 그대로 유지** (지역 필터는 Gold1의 몫) |
| Gold1 | 관련성/유효성 필터 | 지역, "이미 끝난 것" 같은 정적 시점 필터, 제외규칙으로 행/컬럼을 좁힘. 새 파생 수치를 추가하지 않음. **항상 단일 도메인 소유** — 필터 기준이 그 도메인 자신의 필드로 판단되면 그 도메인 것 |
| Gold2 | 점수/지표 계산 | 중심성, capacity, 가중합, percentile 정규화, 최종 합성 점수 등 **새 파생 수치를 만드는 모든 연산**. "조회 시각/요일에 따라 달라지는 동적 활성 여부" 판단도 여기 포함(고정 필터가 아니라 스코어링 로직의 일부) |

핵심 한 줄 규칙: **"기존 행을 좁히기만 하면 Gold1, 새 숫자를 계산해서 만들면 Gold2"** —
도메인이 단일이든 교차든 상관없음. 공용 폴더가 필요한지는 "이 로직 자체가 두 도메인 중
어느 한쪽 소유라고 말할 수 없는 대등한 관계인가"로 판단한다 (Silver2의 조인 로직은 그렇고,
Gold1의 필터는 항상 한쪽이 묻는 질문이라 그렇지 않다).

## 폴더 구조

```
src/
  <domain>/           bronze.py, silver1.py, (silver2.py), (gold1.py), (gold2.py)
  silver2/            교차도메인 조인 (구 mapping/)
  gold2/               교차도메인 점수합성 (구 scoring/, api.py 제외)
  serving/            api.py (구 scoring/api.py, 데이터 레이어 아님)

data/
  bronze/<domain>/
  silver1/<domain>/
  silver2/<domain-or-join-name>/
  gold1/<domain>/
  gold2/<domain-or-join-name>/
```

## 도메인별 마이그레이션 매핑

### construction
- Silver1 (`construction/silver1.py`): 컬럼명통일, 날짜파싱, `resolve_time_chain`(시각복구),
  결측/기간오류 드롭, 도로명정제, 컬럼프루닝, 중복=예외(현행 유지)
- Silver2: 없음 (도메인 자체 조인 없음)
- Gold1 (`construction/gold1.py`): Manhattan필터, 상태/행정허가/차량무관(`DROP_SERIES`)
  제외, 2차 컬럼프루닝 — 구 `gold.py` 그대로 이관
- Gold2: 자체 없음 (교차도메인 `gold2/closure_penalty.py`에서 소비)

### construction_stipulations
- Silver1 (`construction_stipulations/silver1.py`): 정규식+LLM 텍스트 구조화 추출
  (`extract_work_hours`/`extract_work_embargoes`), rule/LLM 병합, dedup, quarantine
- Silver2 (`silver2/construction_work_hours_join.py`, 신규 이동): construction과의
  LEFT JOIN. **construction Gold1이 아니라 construction Silver1과 조인**(전지역 유지
  원칙 — 현재는 construction Gold를 읽고 있어 변경 필요)
- Gold1/Gold2: 없음

### event
- Silver1 (`event/silver1.py`): 컬럼정제, 날짜파싱, 구간파싱(`parse_location`), 컬럼선택,
  중복=예외(현행 유지)
- Silver2 (`silver2/event_lion.py`, 폴더만 이동): 로직 동일, 단 **필터 전 전지역** 대상으로
  매칭하도록 변경(현재는 이미 Manhattan 필터링된 데이터를 매칭 중)
- Gold1 (`event/gold1.py`, 신규): Manhattan필터, run_date 활성필터, `SIDEWALK_ONLY` 제외
  (전부 Silver에서 이관)
- Gold2: 자체 없음 (교차도메인 `gold2/event_boost.py`에서 소비)

### ticketmaster
- Silver1 (`ticketmaster/silver1.py`): 중복컬럼제거, id dedup(조용히 drop, 현행 유지),
  venue JSON파싱(좌표추출), 날짜파싱, 컬럼선택
- Silver2 (`ticketmaster/silver2.py`, 신규): `venue.py`의 `attach_capacity()` 이관
  (현재 고아 모듈을 올바른 위치로만 이동, `event_boost` 연결은 이번 범위 제외)
- Gold1 (`ticketmaster/gold1.py`, 신규): Manhattan bbox필터, run_date 활성필터
  (Silver에서 이관)
- Gold2: 자체 없음

### lion
- Silver1 (`lion/silver1.py`): ogr2ogr 컬럼선택, 수치캐스팅, 도로명정제, SegmentID
  dedup(조용히 drop, 현행 유지)
- Silver2: `lion/silver2.py`(도메인소유, 구 `segment_adjacency.py` — 자기 데이터끼리
  인접관계라 교차도메인 아님) + `silver2/zone_segment.py`(공용, lion×taxi_zone)
- Gold1: 없음 (전체 네트워크를 참조하는 기반데이터라 자체 지역필터 불필요)
- Gold2 (`lion/gold2.py`, 신규): `road_class`/`is_routable`/`capacity_per_hour`/
  `lane_miles`(Silver에서 이관) + 매개중심성/percentile정규화/`traffic_score_v0`
  (구 `traffic_score.py` — **디렉터리를 Silver에서 Gold로 정정**)

### road_closures
- Bronze: 현행 유지 (retention비율 등 무거운 검증은 Bronze 자체 무결성 검증이라 적절)
- Silver1 (`road_closures/silver1.py`): 컬럼명변경, 도로명정제, 날짜캐스팅
- Silver2 (`silver2/road_closure_construction_conflation.py`, 신규 이동): 구 `_combine()`,
  construction Silver1과 겹침판단/conflation
- Gold1/Gold2: 없음 (필요시 추후 추가)

### tlc
- Silver1 (`tlc/silver1.py`): 컬럼통일, 결측컬럼 NULL생성, StructType 스키마 강제,
  컬럼선택, 결측=로그만(현행 유지, 집계 편향 방지 목적의 의도된 예외)
- Silver2: 없음 (도메인 자체 조인 없음)
- Gold1 (`tlc/gold1.py`, 신규): 평일필터 + `zone_id` notna (집계 로직에서 분리)
- Gold2 (`tlc/gold2.py`, 신규): `segment_spatial_weight.py` 이관(구 `mapping/`에 있었으나
  거리역가중+라플라스 스무딩으로 새 수치를 만드는 연산이라 Gold2로 재분류, tlc 전용 소유) +
  `silver2/zone_segment.py`(구조적 매핑)를 소비한 비례분배 + groupby집계 +
  percentile정규화

### taxi_zone
- Bronze: 필수컬럼/notna/유니크/row-count범위 검증 중 **존재 여부 확인만** 남김
- Silver1 (`taxi_zone/silver1.py`, 신규): 위 검증의 나머지(필수컬럼, notna, 유니크,
  row-count범위, feature count)를 이관
- Silver2: 해당 없음 — 대신 `silver2/zone_segment.py`가 taxi_zone **Silver1**을 읽도록
  변경(현재는 Bronze를 직접 읽음)
- Gold1: `get_manhattan_zone_ids()` 고아 헬퍼 — 사용하면 `taxi_zone/gold1.py`로,
  안 쓰면 삭제 (구현 단계에서 결정)

### 교차도메인 Gold2 (`src/gold2/`, 구 `scoring/`)
- `closure_penalty.py`, `event_boost.py`, `traffic_score.py` — 로직/파일명 변경 없음,
  저장 경로만 `GOLD_DIR` 기준으로 정정 (현재 `closure_penalty.py`가 `SILVER_DIR`에 저장 중)

### 서빙 (`src/serving/`)
- `api.py` (구 `scoring/api.py`) — 데이터 레이어가 아닌 HTTP 서빙 계층이라 분리

## 크로스도메인 참조 규칙

- Silver2(조인)는 항상 다른 도메인의 **Silver1**을 읽는다 (Gold를 읽지 않는다 — 전 지역
  유지 원칙과 충돌하기 때문)
- Gold1(필터)은 자기 도메인의 Silver2(있으면) 또는 Silver1 출력을 필터링한다
- Gold2(점수)는 여러 도메인의 **Gold1** 출력(또는 Gold1이 없는 도메인은 Silver1/Silver2)을
  조합한다
- 여러 Gold2 산출물을 다시 합치는 "최종 합성" 성격의 Gold2(예: `traffic_score.py`가 lion의
  centrality/capacity, tlc의 volume, closure_penalty, event_boost를 전부 합산)는 **다른
  도메인의 Gold2 출력을 입력으로 받아도 된다** — Gold2끼리는 서로 참조 가능하다. 단
  Gold1/Silver 레이어가 Gold2를 거슬러 읽는 것은 금지(항상 상위 레이어만 하위를 참조)

## 남은 결정 사항

- tlc의 결측치 정책은 그대로 유지하기로 확정(위 "제외" 참고)
- `venue.py` capacity 연결, `get_manhattan_zone_ids()` 처리, DAG task 이름 세부 사항은
  구현 계획 단계에서 다룬다
