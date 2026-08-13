# TLC 세그먼트x시간대 통행량 Gold 설계

## 배경

이 프로젝트(현대자동차 소프티어 8기 DE 5조, NYC 도로 공사 허가 최적화)의 최종 목표는
"공사 허가 신청이 들어오면, 기존 교통 영향을 계산해서 공사 시간대를 추천"하는 것이다.
뉴욕시는 도로 공사 허가 시 교통 영향을 최소화해야 한다고 명시하고 있어서, 신청서에 적힌
구간·기간에 대해 시간대별 traffic score를 보여주는 기능이 필요하다.

팀 내에서 이미 만들어진 산출물:

- `map_zone_segment.parquet` — LION 세그먼트 ↔ TLC 존(zone) 1:1 매핑
- `dim_segment_traffic_score_v0.parquet` — 세그먼트별 매개중심성 기반 demand, capacity_per_hour
- `graph_segment_adjacency.parquet` — 세그먼트 인접 그래프(교차로 노드 공유 기준, 양방향)
- `src/scoring/traffic_score.py` — 여러 요인을 가중치 설정(`config/traffic_score_weights.yaml`)으로
  조합해 traffic_score를 계산하는 조회 인터페이스. `tlc_volume`, `event_boost`, `closure_penalty`는
  아직 `enabled: false`인 스텁 상태이며, `ts_hour` 파라미터는 지금 받기만 하고 계산에 안 쓴다
  (정적 점수).

이번 설계는 이 중 **TLC 데이터를 가공해 `tlc_volume` 컴포넌트를 만드는 부분**을 다룬다. TLC
데이터는 "택시 수요"가 아니라 "일반적인 도로 교통량 프록시"로 간주한다.

## 목표

세그먼트별로 "평일 시간대(0~23시)마다 이 구간이 상대적으로 얼마나 붐비는가"를 나타내는
Gold 테이블을 만들고, 공사 허가 신청서의 특정 세그먼트(및 인접 세그먼트)에 대해 이 값을
조회할 수 있는 인터페이스를 제공한다.

**대상 지역은 맨해튼으로 한정한다** (공사 허가 신청 자체가 맨해튼 대상). `map_zone_segment`의
`borough` 컬럼으로 필터링하며, `src/tlc/transform.py`(TLC silver, 팀 공용 코드) 자체는
건드리지 않는다 — 그 파일은 도시 전체를 다루는 공용 자산이라, 맨해튼 한정은 이 설계의
Gold 단계에서만 적용한다.

## 범위

**포함**

- `dim_segment_tlc_volume.parquet` Gold 테이블 생성 로직 + 검증 함수
- 세그먼트 + 시간대를 입력받아, 인접 3단계 이내 세그먼트까지 TLC 기반 점수를 반환하는
  임시 조회 함수

**제외 (후속 작업)**

- `src/scoring/traffic_score.py`/`config/traffic_score_weights.yaml`을 고쳐서
  `tlc_volume`을 다른 컴포넌트(중심성, capacity, event, closure)와 실제로 합치는 것.
  이 설계는 팀 공용 코드는 건드리지 않고, 같은 관례(세그먼트별 0~1 정규화 값)를 따르는
  독립된 산출물만 만든다.
- 공사 허가 신청서의 도로명/교차로/WKT를 `segment_id`로 변환하는 로직. 이 설계의 조회
  함수는 `segment_id`를 이미 입력받는다고 가정한다.
- 주말 처리. 주말 공사는 별도 허가 프로세스라 이번 범위에서는 평일만 다룬다. 주말 공사
  허가가 생기면 그때 추가로 설계한다.
- Airflow DAG 연결. `dim_segment_traffic_score_v0`(중심성)와 동일하게, 지금은 직접 실행하는
  스크립트 형태로만 만든다.

## 데이터 흐름

```
data/silver/{yellow,green,fhv,fhvhv}_tripdata_*/  (약 140개 파일, 3년치)
        │  dropoff_datetime, dropoff_location_id만 선택
        ▼
평일(월~금)만 필터 + dropoff_datetime에서 hour(0~23) 추출
        │
        ▼  (dropoff_location_id, hour) 기준 group by count
zone_id x hour 하차수 집계  (최대 263 zone x 24시간 = 6,312행)
        │
        ▼  map_zone_segment.parquet과 join (zone 총합을 그 zone의 모든 세그먼트에 동일 복사)
        ▼  routable 세그먼트 x 0~23시 풀 그리드에 left join, 미매치는 0으로 채움
segment_id x hour 하차수  (맨해튼 세그먼트 수 x 24행)
        │
        ▼  전체 기준 global percentile rank (0~1)
dim_segment_tlc_volume.parquet
```

zone 단위로 먼저 집계한 뒤 세그먼트로 펼친다. 반대 순서(트립 단위로 세그먼트에 먼저
조인)로 하면 트립 레코드가 세그먼트 수만큼 뻥튀기되어 훨씬 비효율적이다.

## Gold 테이블 스키마

`data/silver/dim_segment_tlc_volume.parquet` (`dim_segment_traffic_score_v0.parquet`과 동일한
위치/네이밍 관례)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `segment_id` | string | LION 세그먼트 ID (routable만) |
| `hour` | int (0~23) | 시간대 (평일 기준) |
| `dropoff_count_raw` | long | 세그먼트가 속한 zone의 평일 해당 시간대 총 하차수 (같은 zone의 모든 세그먼트가 동일값) |
| `tlc_volume` | double (0~1) | `dropoff_count_raw`를 전체 (segment_id, hour) 조합 기준 global percentile rank로 정규화한 값 |

세그먼트 하나당 정확히 24행(트립이 0건인 시간대도 0으로 채워 행을 유지). 전체 행 수 =
맨해튼 세그먼트 수(약 19,574개) x 24 ≈ 469,776행.

## 계산 로직

1. **재계산 방식**: 실행할 때마다 그 시점에 존재하는 TLC silver 파일 전부를 다시 읽어
   처음부터 계산한다(증분 아님). 이유:
   - 기존 Gold 테이블(`dim_segment_traffic_score_v0`, `graph_segment_adjacency`)도 전부 이
     방식이라 프로젝트 관례와 일치한다.
   - 최종 집계 결과가 작아서(zone 263개 x 시간대 24개가 중간 집계 결과) 3년치를 매번
     다시 훑어도 Spark 연산상 문제없다. `groupBy(...).count()`는 파티션별 부분 집계 후
     작은 결과만 셔플하는 구조라, 원본 데이터 크기와 무관하게 메모리 사용량이 작다.
   - TLC 데이터 자체가 "최근 3년치만 유지, 오래된 건 별도로 삭제"되는 롤링 윈도우로
     관리되므로, 이 Gold 잡은 그 시점에 실버 폴더에 있는 파일 전부를 읽으면 되고 별도로
     "3년" 기준을 코드에서 계산할 필요가 없다. 오히려 증분 방식은 윈도우에서 빠지는
     오래된 파일의 기여분을 빼는 로직까지 필요해 훨씬 복잡해진다.
2. **소스 컬럼**: `dropoff_datetime`, `dropoff_location_id` (TLC silver 공통 스키마,
   `src/tlc/transform.py` 참고)만 선택해서 읽는다.
3. **평일 필터**: `dropoff_datetime`의 요일이 월~금인 행만 남긴다.
4. **시간 추출**: `dropoff_datetime`에서 `hour`(0~23)를 뽑는다.
5. **zone 단위 집계**: `(dropoff_location_id, hour)` 기준 group by count → 작은 중간 결과.
6. **zone → segment 펼치기**: `map_zone_segment.parquet`(`segment_id`, `zone_id`, `borough`)에서
   먼저 `borough == "Manhattan"`인 세그먼트만 남긴 뒤, `dropoff_location_id == zone_id`로
   join한다. 하나의 zone에 여러 세그먼트가 속하면, zone의 하차수 총합을 그 세그먼트
   전부에 동일하게 복사한다(세그먼트 수로 나누지 않음).
7. **빈 시간대 채우기**: routable 세그먼트 전체 x 0~23시 풀 그리드를 만들고 6번 결과를
   left join, 매치 안 된 칸은 `dropoff_count_raw = 0`으로 채운다.
8. **정규화**: `dropoff_count_raw`를 전체 (segment_id, hour) 조합(약 377만 행) 기준으로
   한 번에 `rank(pct=True, method="average")`를 매겨 `tlc_volume`(0~1)을 만든다. 세그먼트별로
   따로 순위를 매기는 게 아니라, 24시간 전체를 하나로 묶어 비교한다. 이는
   `dim_segment_traffic_score_v0`의 `demand_raw`(중심성)를 만들 때 쓴 방식과 동일하다.

   **왜 정규화가 필요한가**: 최종 traffic_score는 `(demand 요인들의 가중합) / capacity`
   형태다. demand 쪽에는 중심성(그래프 이론 값, 물리적 단위 없음)과 tlc_volume(하차
   건수, 물리적 단위 있고 숫자도 훨씬 큼)처럼 태생이 다른 값들이 섞인다. 정규화 없이
   그대로 더하면 숫자가 큰 쪽이 가중치 설정과 무관하게 결과를 압도한다. 두 값을 모두
   0~1 percentile rank로 맞춰야 `traffic_score_weights.yaml`의 가중치 설정이 의미를
   가진다. (반대로 capacity 쪽은 물리적 단위를 그대로 쓰는 게 맞고, 이 설계에서 바꾸지
   않는다.)
9. **제외 대상**:
   - 맨해튼이 아닌 세그먼트(다른 4개 자치구)는 제외
   - routable이 아니거나 `map_zone_segment`에 없는 세그먼트는 처음부터 제외
     (`dim_segment_traffic_score_v0`와 동일한 대상 범위)
   - TLC의 특수 zone 코드(264 "N/V" 등 미상, 265 NYC 밖 등)는 zone_id 1~263 범위 밖이라
     join에서 자연히 빠진다. 몇 건이 빠졌는지만 로그로 남긴다.

## 검증 로직

`dim_segment_traffic_score_v0`의 `validate_dim_segment_traffic_score()`와 동일한 패턴:

- `(segment_id, hour)` 조합 중복 없음
- 세그먼트마다 정확히 24행 (0~23시 전부 존재)
- `tlc_volume`은 0~1 범위
- `dropoff_count_raw`는 0 이상
- 전체 행 수가 예상 범위(맨해튼 세그먼트 수 x 24) 안에 있음

## 조회 함수

공사 신청서에 적힌 세그먼트 하나(및 그 주변)의 TLC 기반 점수를 바로 볼 수 있는 임시
인터페이스. 나중에 팀 공용 `scoring/traffic_score.py`가 여러 요인을 합칠 때 이 값을
가져다 쓸 수 있도록, 같은 정규화 관례(0~1)를 따르는 독립된 함수로 만든다.

- **입력**: `segment_id`(공사 위치), `hour`(0~23)
- **처리**:
  1. `graph_segment_adjacency.parquet`으로 무방향 그래프를 만든다.
  2. 입력 `segment_id`로부터 3단계 이내(자기 자신 포함)에 있는 세그먼트 집합을 BFS로 찾는다.
     세그먼트당 평균 이웃이 2.8개 수준이라(팀원 확인) 3단계를 뻗어도 보통 결과는 수십 개
     이내다.
  3. 그 세그먼트들 각각에 대해 `dim_segment_tlc_volume.parquet`에서 `hour`가 일치하는
     `tlc_volume`을 조회한다.
- **출력**: `{segment_id, hop_distance, hour, traffic_score}` 리스트. `traffic_score`는
  지금은 `tlc_volume` 값 그대로다(TLC 단일 요인만 반영한 임시 점수).
- **예외**: 입력 `segment_id`가 routable 대상에 없거나 `hour`가 0~23 밖이면 에러.

## 구현 위치 제안

`src/lion/traffic_score.py`(중심성 Gold 테이블)와 같은 관례를 따라, TLC 도메인 폴더 안에
둔다:

- `src/tlc/gold.py` — `build_dim_segment_tlc_volume()`, `validate_dim_segment_tlc_volume()`,
  조회 함수(예: `get_tlc_traffic_score_for_construction()`) 전부 이 파일 하나에 둔다.
- 인접 세그먼트(3단계) 탐색 로직은 별도 공용 유틸리티로 분리하지 않고, 이 조회 함수
  안에 포함시킨다. `graph_segment_adjacency.parquet`은 그대로 재사용하되, 이 그래프를
  "다른 컴포넌트(예: closure_penalty)도 나중에 재사용할 수 있게 분리할지"는 이번 범위에
  넣지 않는다 — 필요해지면 그때 팀원과 논의해서 공용화한다.

구체적인 함수 시그니처·내부 구현 순서는 다음 단계(구현 계획)에서 정한다.

## 향후 확장 (이번 설계 범위 밖)

- `scoring/traffic_score.py`의 `COMPONENT_SOURCES`/`get_traffic_score()`가 실제로 `ts_hour`를
  써서 이 Gold 테이블을 조회하도록 연결하는 것 (다른 컴포넌트들과 통합).
- 공사 허가 신청서(도로명/교차로/WKT) → `segment_id` 변환.
- 이 Gold 잡을 Airflow DAG에 정기 실행으로 연결.
- 주말 공사 허가 프로세스가 생기면 주말 데이터 처리 추가.
