# Traffic Score 산정 방식

이 문서는 세그먼트(도로 구간) x 시간대별 `traffic_score`가 실제로 어떻게 계산되는지,
각 구성 요소와 상수의 근거를 실제 데이터 예시와 함께 정리한다. 코드가 바뀌면 이
문서도 같이 갱신해야 한다 — 특히 `config/traffic_score_weights.yaml`과
`src/scoring/closure_penalty.py`의 상수들.

## 1. 전체 공식

```
demand   = Σ (weight × value)  (활성화된 demand 컴포넌트들)
capacity = Σ (weight × value)  (활성화된 capacity 컴포넌트들)
traffic_score = demand / capacity
```

가중치/활성화 여부는 `config/traffic_score_weights.yaml`에서 관리한다(코드 수정 없이
컴포넌트 추가/on-off 가능):

```yaml
components:
  demand:
    centrality:   { weight: 1.0, enabled: true }
    tlc_volume:   { weight: 1.0, enabled: true }
    event_boost:  { weight: 0.3, enabled: true }
  capacity:
    base_capacity:    { weight: 1.0, enabled: true }
    closure_penalty:  { weight: 1.0, enabled: true }
```

실제 계산은 `src/scoring/traffic_score.py`의 `get_traffic_score()`가 담당한다.

## 2. Demand 쪽 — 전부 도시 전체 percentile

| 컴포넌트 | 데이터 소스 | 계산 방식 |
|---|---|---|
| `centrality` | `dim_segment_traffic_score_v0.demand_raw` (`src/lion/traffic_score.py`) | 맨해튼 도로망 그래프의 betweenness centrality(근사, k=1000)를 구해서 **전체 세그먼트 기준 percentile rank(0~1)**로 정규화 |
| `tlc_volume` | `dim_segment_tlc_volume.tlc_volume` (`src/tlc/gold.py`) | TLC 택시 하차 건수를 **전체 (segment, hour) 조합 기준 percentile rank**로 정규화. `dags/tlc_gold_volume.py`를 아직 안 돌렸으면 0으로 처리 |
| `event_boost` | `dim_segment_event_boost.event_boost` (`src/scoring/event_boost.py`) | 행사(event)/티켓마스터 공연장 근접 세그먼트에 가중치 부여 |

**중요**: 셋 다 "이 도로가 전체 중 상위 몇 %냐"는 **도시 전체 기준**이지, 세그먼트
자기 자신 안에서의 상대값이 아니다. (과거에 세그먼트별 min-max 정규화를 시도했다가
"공사 전/후 스케일이 달라 보이는 착시" 문제로 되돌린 이력이 있다 —
`dashboard/index.html:885-891` 주석 참고.)

## 3. Capacity 쪽 — base_capacity + closure_penalty

`base_capacity`는 `dim_segment_traffic_score_v0.capacity_per_hour` (차로 수 등 도로
자체 속성 기반, 분기 1회 정적값). 맨해튼 세그먼트의 약 15%는 LION 원본에 이 값이
없어 결측이다.

`closure_penalty`가 공사/도로통제 영향을 반영하는 핵심 로직이다(아래 4번).

## 4. closure_penalty 계산 (`src/scoring/closure_penalty.py`)

### 4.1 활성 여부 판단

공사/폐쇄 허가(construction + road_closures 병합, 물리적 현장 기준 중복 제거) 중
"이 날짜, 이 시각에 실제로 작업 중인 것"만 골라낸다:

1. **날짜**: `query_date`가 permit의 `[work_start_ts, work_end_ts]` 범위 안에 있는지
2. **시각**: `work_start_hour`~`work_end_hour` 구간 안에 있는지 (자정 넘기는 야간
   구간, 예: 22시~6시도 처리)
3. **요일**: `work_days_code`(WEEKDAY/WEEKEND/SATURDAY/SUNDAY/DAILY/EXCEPT_SUNDAY)가
   `query_date`의 요일과 맞는지

시간대/요일 정보가 없거나 파싱 실패(`OTHER`)면 "항상 활성"으로 보수적으로 취급한다.

**embargo(행사 때문에 작업이 일시 중단되는 기간)는 여기 반영하지 않는다** — 한 번
시도했다가 되돌렸다. embargo는 연중 특정 날짜에만 있는 예외적 사건이라, 이걸
traffic_score(평소 패턴을 보여주는 지표)에 반영하면 "그날 마침 대형 행사가
있었는지"에 따라 왜곡된다(실측: Summer Streets embargo가 걸린 날 하루 전체 공사
영향이 0으로 보였음). embargo 정보는 `get_newly_issued_closures()`에서 참고 정보로만
보여준다.

### 4.2 홉 전파 (spatial spread)

활성 레코드를 segment별 개수(`intensity`)로 집계한 뒤, 인접 세그먼트로 최대
`MAX_HOPS=3`까지 BFS로 전파하며 `HOP_DECAY`로 가중 누적한다:

```python
HOP_DECAY = {0: 1.0, 1: 0.85, 2: 0.65, 3: 0.4}
```

한 세그먼트가 여러 진앙(ground zero)의 영향권에 겹치면 각 진앙의
`(intensity × hop_decay)` 기여도를 전부 합산한다.

> **주의**: 이 감쇠값 자체는 실측 검증된 게 아니라 "홉이 멀수록 영향이 줄어든다"는
> 정성적 요구만 반영한 초안이다(TODO, 팀 검토 필요). 2026-08월 개정에서 기존
> `{0:1.0, 1:0.75, 2:0.5, 3:0.25}`보다 값을 올렸다 — 진앙 자체의 최대 감소율에
> 상한(아래 4.4)을 씌운 대신, 그 도로를 못 지나가는 차량이 실제로는 주변 도로로
> 우회하는 효과를 반영하기 위해서다.

### 4.3 차로 수 기반 용량 감소 (NCHRP Report 03-107)

누적된 강도(`intensity`)를 실제 용량 감소량으로 변환하는 공식:

```
reduction_ratio = URBAN_WORK_ZONE_MAX_REDUCTION × intensity / (intensity + K)
reduction       = -(capacity × reduction_ratio)
```

포화 곡선(intensity가 아무리 커도 상한 이상 안 깎임) 형태 자체는 유지하되, 두
상수(`K`, 상한)를 **HCM(Highway Capacity Manual) / NCHRP Report 03-107**(작업구간
용량 방법론, FREEVAL-WZ가 쓰는 실측 근거)의 관측치로 보정했다:

- **K (half-saturation intensity)**: 세그먼트의 실제 차로 수(`lanes_total`, LION
  데이터)를 반영한다. NCHRP 실측: "다차선 도로에서 차로 1개만 남았을 때 용량이
  68%(감소 32%)로 떨어진다." `intensity`가 늘어나 "차로 1개만 남은 것"과 같아지는
  지점에서 정확히 이 32% 감소가 나오도록 세그먼트별로 `K`를 역산한다:

  ```python
  K = (lanes_total - 1) × NCHRP_ONE_LANE_OPEN_CAF / (1 - NCHRP_ONE_LANE_OPEN_CAF)
    = (lanes_total - 1) × 0.68 / 0.32
    = (lanes_total - 1) × 2.125
  ```

  차로가 적을수록 `K`가 작아서 같은 `intensity`에도 더 빨리 포화(=영향을 더 크게
  받는다)는 뜻이다. `lanes_total=1`(편도 1차로)이면 `K≈0` — 공사가 하나라도 겹치면
  거의 즉시 상한에 도달한다. `lanes_total`을 모르는 세그먼트(약 15%)는 기존 고정값
  (`HALF_SATURATION_INTENSITY≈2.33`, `PENALTY_RATIO=0.3` 기준)으로 폴백한다.

- **상한 (`URBAN_WORK_ZONE_MAX_REDUCTION = 0.58`)**: `intensity`가 아무리 커져도
  `reduction_ratio`가 100%(완전 폐쇄)까지 가지 않도록 캡을 씌운다. 100% 완전폐쇄는
  어떤 레퍼런스로도 검증되지 않은 극단값이라(NCHRP의 32% 수치는 "다차선 도로가
  1차로까지 좁아진" 경우만 측정한 것이지, 원래 1차로였던 도로 자체를 측정한 게
  아니다), 대신 HCM 도심 도로(urban street) 실측 연구 중 "미드블록 작업구간 존재
  시 관측된 심각한 사례"(58% 감소, 1,040 vphpl)를 절대 상한으로 쓴다. NYC
  맨해튼처럼 도심 가로망(freeway 아님)인 이 프로젝트 맥락에 더 맞는 수치이기도
  하다.

**개정 이력(2026-08)**: 원래는 `PENALTY_RATIO=0.3`(고정, "공사 1건이면 30% 감소"라는
근거 없는 추정)이었다. NCHRP 차로 기반 공식으로 바꾼 직후 실측해보니, 맨해튼
세그먼트의 25%가 `lanes_total=1`이라 영향받는 (segment, hour) 조합의 35%가 99% 이상
감소(사실상 완전폐쇄)로 나오는 문제가 있었다 — 이건 상한 없이 K만 바꿨을 때
생긴 부작용이었고, 위 58% 상한을 추가해서 해결했다. 상한 적용 후 재실측: 최대
58.0%, 평균 40.5%, 중앙값 46.7%.

### 4.4 등장하는 상수 요약

| 상수 | 값 | 근거 |
|---|---|---|
| `MAX_HOPS` | 3 | 정성적 초안(TODO) |
| `HOP_DECAY` | `{0:1.0, 1:0.85, 2:0.65, 3:0.4}` | 정성적 초안(TODO), 4.3의 상한 도입에 맞춰 상향 조정 |
| `NCHRP_ONE_LANE_OPEN_CAF` | 0.68 | NCHRP Report 03-107 실측(다차선 도로, 차로 1개 남았을 때) |
| `URBAN_WORK_ZONE_MAX_REDUCTION` | 0.58 | HCM 도심 도로 실측(미드블록 작업구간, "typical하진 않다"는 단서 있음) |
| `HALF_SATURATION_INTENSITY` | ≈2.33 | `lanes_total` 결측 세그먼트용 폴백(옛 `PENALTY_RATIO=0.3` 기준) |

## 5. 실제 데이터로 예시 (`0077356`, 2026-08-18 08시)

세그먼트 `0077356`은 `lanes_total=1`(편도 1차로), `base_capacity=600`.

**Before (공사 영향 없다고 가정, `include_closure_penalty=False`):**
```
demand   = centrality(0.6916) + tlc_volume(0) + event_boost(0) = 0.6916
capacity = base_capacity(600)
score = 0.6916 / 600 = 0.001153
```

**After (실제 공사 반영, 2026-08 상수 개정 후):**
```
capacity = base_capacity(600) + closure_penalty(-348.0) = 252.0
score = 0.6916 / 252.0 = 0.002744
```

`closure_penalty = -348.0`이 나온 과정:
- 이 세그먼트에 겹치는 활성 공사/통제의 홉 감쇠 가중합(`intensity`)이 충분히 커서
  포화 상한에 근접
- `lanes_total=1`이라 `K≈0` → `intensity`가 조금만 있어도 빠르게 포화
- `reduction_ratio = 0.58 × (거의 1.0) ≈ 0.58` (상한 58%에 도달)
- `reduction = -(600 × 0.58) = -348.0`

즉 **1차로 도로에 공사가 겹치면 사실상 상한(58%)까지 바로 깎인다** — 완전폐쇄까지는
아니지만 상당히 심각한 영향으로 취급한다.

## 6. 알려진 한계 (TODO)

1. **demand와 capacity의 단위가 다르다.** `capacity`는 실제 차량/시간 단위
   (`capacity_per_hour`)인데, `demand`는 percentile rank의 가중합(무차원, 0~1대)이다.
   전통적인 V/C(Volume-to-Capacity) ratio는 둘이 같은 단위라는 전제인데, 지금은 그
   전제가 안 맞는다. `traffic_score = demand/capacity`가 실제 "V/C ratio"의 의미를
   완전히 가지려면 demand 쪽도 실제 교통량 단위로 재구성해야 한다 — 이번 개정
   범위 밖.
2. **BPR 함수(Bureau of Public Roads volume-delay function, `1+0.15×(V/C)^4`) 적용은
   보류했다.** 표준 교통배정 모델에서 V/C ratio를 지연/혼잡 지수로 바꿀 때 널리
   쓰이는 공식인데, 실제로 이 프로젝트의 `demand/capacity` 값(예: 0.001~0.003대)에
   적용해보면 4제곱 때문에 거의 0에 수렴해서 `1+0.15×(작은값)^4 ≈ 1.0000000...`으로
   전 세그먼트가 사실상 동일한 값이 되어버린다 — 1번 한계(단위 불일치) 때문에
   BPR이 원래 가정하는 "V/C가 0~1대 스케일"이라는 전제 자체가 안 맞아서 생기는
   문제다. demand 쪽을 실제 교통량 단위로 재구성하기 전엔 의미 있게 적용하기
   어렵다.
3. **HOP_DECAY, MAX_HOPS는 여전히 정성적 추정이다.** 4.3에서 두 상수(K, 상한)는
   실측 레퍼런스로 보정했지만, "홉이 멀수록 영향이 얼마나 줄어드는지"는 아직
   근거 있는 값이 아니다.
4. **`lanes_total` 결측(맨해튼 세그먼트의 약 15%)** 은 옛 고정값으로 폴백한다 —
   가능하면 LION 원본 데이터를 보강해서 결측을 줄이는 게 근본적인 개선이다.

## 참고 자료

- Highway Capacity Manual (HCM) — 작업구간(work zone) 용량 방법론, 7th Edition
  Equations 10-8/10-9 (도심 도로)
- NCHRP Report 03-107 — "Work Zone Capacity Methods", 차로 수 기반 용량 조정계수(CAF)
- Bureau of Public Roads (BPR) volume-delay function — α=0.15, β=4 (표준 교통배정
  모델에서 널리 쓰임, 이 프로젝트엔 6번 한계로 미적용)
