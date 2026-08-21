# 내비게이션 골드 데이터셋 파이프라인 설계

## 배경

기존 파이프라인은 8개 도메인(construction, construction_stipulations, event, lion, road_closures,
ticketmaster, tlc, taxi_zone)을 합성해 대시보드용 `traffic_score`(demand/capacity 기반 혼잡도
단일 지표)를 만드는 게 목적이었다. 이번에 새로 만드는 건 목적 자체가 다르다 — **네비게이션
API가 경로 계산(최단 시간/최단 거리/최소 통행료 등)에 바로 쓸 수 있는, segment별 원시 값**을
제공하는 것이다. "하나의 합성 점수"가 아니라 "타입별로 분리된, 그 자체로 의미 있는 값"이
목표라 기존 traffic_score 체계와는 양립하지 않는다.

**결정: 기존 `traffic_score` 파이프라인은 폐기하고 이 신규 파이프라인으로 완전히 대체한다.**
메달리온 아키텍처(Bronze/Silver1/Silver2/Gold1/Gold2)와 서빙 인프라(RDS 패턴, 로컬 개발
환경)는 그대로 유지한다.

## 목표

segment_id × type × (날짜+30분 슬롯) 단위로 값을 저장해두고, 다음 API 계약으로 서빙한다:

```
요청: [[segment_id, segment_id, ...], type, time]
응답: [value, value, ...]   # 요청한 segment 순서 그대로, 개수 동일
```

1차 구현 대상은 두 타입:
- **type=1 (시간)**: 그 segment를 지나가는 데 걸리는 시간
- **type=2 (거리)**: 그 segment의 길이

이후 타입(혼잡도/통행료 등 후속 확장)은 이번 범위 밖이며, 같은 패턴(도메인이 자기 타입을
소유, 독립된 골드 테이블)을 따라 추가한다.

## 설계 원칙

- **무결점 응답 > 신뢰성**: 어떤 (segment, type, time) 조합이든 API는 반드시 값을 반환한다.
  null이나 에러로 "모르겠다"고 답하지 않는다. 구체적인 fallback 로직/값은 이번 문서 범위
  밖이며 구현 계획 단계에서 정한다 — 단, 이 우선순위는 스키마/검증 설계에 계속 반영돼야 한다.
- **타입별 소유권 분리**: 각 타입은 정확히 하나의 도메인이 소유한다. 여러 도메인 값을
  가중합해서 새 합성 지표를 만드는 traffic_score식 "대등한 관계의 cross-domain 로직"은
  이번 파이프라인엔 없다 — type=1(시간)은 `speed` 도메인이, type=2(거리)는 `lion` 도메인이
  전적으로 소유하고, 필요한 다른 도메인 데이터(예: speed가 쓰는 lion의 길이)는 그냥 참조만
  한다(기존 `tlc/gold2.py`가 `lion.gold2.DIM_SEGMENT_PATH`를 직접 import하는 것과 동일한
  패턴). 별도의 "cross-domain 합성 폴더"는 만들지 않는다.
- **계산은 배치(Gold2)에서 끝내고 서빙은 조회만**: type=1(시간) = 길이 ÷ 속도 계산도
  Gold2 단계에서 미리 끝내서 최종값을 저장한다. 서빙 API는 (segment_id, type, time) 키로
  이미 계산된 값을 조회만 할 뿐 요청 경로에서 계산/나눗셈을 하지 않는다 — 무결점 응답을
  요청 경로가 아니라 배치 단계에서 검증/보장하기 위함이다.
- **지역 필터 없음**: 기존 traffic_score가 맨해튼으로 한정했던 것과 달리, 이 파이프라인은
  별도 지역 필터를 두지 않는다(LION/DOT 데이터가 커버하는 범위 그대로).

## 데이터 키

`(segment_id, type, date, time_slot)` — `date`는 특정 캘린더 날짜, `time_slot`은 30분
단위(예: 12:00~12:30). 요일 기반 반복 패턴이 아니라 **실제 날짜별로 별도 row**를 둔다.

이 키 설계는 테이블이 시간이 지날수록 계속 쌓인다는 뜻이다 — 보존/생성 범위(과거 며칠,
미래 몇 시간을 미리 계산해둘지) 정책은 미결정 사항(아래 참고)이다.

## 타입별 소스와 레이어별 처리

| Layer | 속도(→type=1 시간) | LION(→type=2 거리) |
|---|---|---|
| Bronze | link_id 기준 원본 5분 판독값 그대로 저장 (link_points 위경도 포함) | 원본 shapefile 그대로 |
| Silver1 | 정제(타입 캐스팅, 결측/이상치 제거) — 지역 필터 없음 | 정제(컬럼 정제, SegmentID dedup) |
| Silver2 | link_points를 LION 좌표계로 재투영 → LION segment geometry와 buffer+STRtree 매칭 → link_id↔segment_id 매핑 테이블(값 없음, 순수 구조) | 해당없음 |
| Gold1 | 해당없음 | 해당없음 |
| Gold2 | Silver1(값)×Silver2(매핑) 조인 → link 값을 segment로 펼침 → 30분 단위 가중평균 → lion 길이(직접 참조)로 나눠 **최종 시간값**까지 계산·저장 | `dim_segment` 완성본(length_ft, capacity_per_hour, is_routable 등) — **centrality/traffic_score 관련 계산은 제거** |
| 서빙 | 저장된 시간값 조회만 | 저장된 길이값 조회만 |

### 속도(speed) 도메인 상세

- **수집 주기**: NYC DOT 소스는 5분 간격으로 갱신된다. Bronze는 5분마다 폴링하지 않고
  **30분에 한 번, 지난 30분 범위(`data_as_of` 기준 WHERE절)를 한 번의 API 호출로 수집**한다
  (Socrata range 쿼리, 기존 construction_stipulations/speed 초안의 일별 range 패턴과 동일한
  방식을 30분 단위로 적용). 수집된 5분 단위 판독값은 Bronze에 개별 row로 그대로 저장한다
  (Bronze는 원본 그대로 저장 원칙 유지, 미리 뭉개지 않음).
- **30분 대표값 계산**: 한 구간(최대 6개 5분 판독값)에 **선형 증가 가중치**(1, 2, ..., n을
  합으로 정규화, 가장 최근 값이 최대 가중치)를 적용한 가중평균을 Gold2에서 계산한다. 판독값이
  6개보다 적으면 있는 것들끼리 순서대로 재정규화한다.
  > TODO(팀 검토 필요): 선형 가중은 파라미터가 없어 우선 채택한 정성적 초안이다. 실측 후
  > 지수가중(EWMA) 등으로 교체할 수 있다.
- **시간 계산**: 위 가중평균 속도값을 `lion.gold2`가 만든 `dim_segment.length_ft`로 나눠
  segment당 최종 통행 시간을 계산하고, 그 값을 저장한다.

## 재사용 / 폐기 매트릭스

**그대로 유지**
- 메달리온 레이어 정의·폴더 규칙(`src/<domain>/{bronze,silver1,silver2,gold1,gold2}.py`,
  `data/<layer>/<domain>/`)
- `src/common/*` 전부: `db.py`의 RDS 서빙 패턴(특히 `write_partitioned_table`/`read_partition`의
  `dt` 파티션 방식은 날짜별 스냅샷 저장에 그대로 재사용), `config.py`의 APP_ENV local/aws 토글,
  `socrata.py`, `spark.py`, `logger.py`, `alerts.py`, `gx.py`
- `docker-compose.yml`(로컬 Postgres `rds-local`), `Dockerfile`, `.github/workflows/`,
  `pytest.ini`, TDD/커밋 관례
- `lion/gold2.py`의 `build_dim_segment`(road_class/length_ft/capacity_per_hour/is_routable 계산)
- 공간 매칭 기법(패턴만 재사용): `tlc/gold2.py`/ticketmaster 매핑의 buffer+STRtree 방식

**폐기 (chore/nav-retire-old에서 즉시 삭제, git 히스토리로 충분히 복구 가능하므로 격리 없이 삭제)**
- `src/gold2/traffic_score.py`, `src/gold2/closure_penalty.py`, `src/gold2/event_boost.py`
- `src/serving/api.py`, `dashboard/`
- `dags/gold_closure_penalty.py`
- `config/traffic_score_weights.yaml`, `docs/traffic_score_methodology.md`
- `docs/superpowers/plans/2026-08-20-speed-baseline-pipeline.md`(미구현 상태로 폐기, 날짜+30분
  구조로 새로 설계)
- `lion/silver2.py`(segment adjacency 그래프 — traffic_score의 centrality 계산 전용)
- `lion/gold2.py`의 centrality 관련 부분(`build_dim_segment_traffic_score`,
  `validate_dim_segment_traffic_score`, `DIM_SEGMENT_TRAFFIC_SCORE_PATH`, `BETWEENNESS_*` 상수) —
  `build_dim_segment`는 남기고 이 부분만 제거

**지금 범위 아님, 건드리지 않고 보존 (나중 확장 후보)**
- `construction`, `construction_stipulations`, `event`, `road_closures`, `ticketmaster`,
  `taxi_zone`, `tlc` 도메인 전체 — 나중에 새 type(혼잡도/통행료 등) 추가 시 참고할 원천

## 폴더 구조

```
src/
  speed/                    # 신규(재설계)
    __init__.py
    bronze.py               # 30분마다 지난 30분치 배치 수집(5분 단위 원본 그대로 저장)
    silver1.py              # 정제(지역 필터 없음)
    gold2.py                # 30분 가중평균 + lion 길이 참조 -> 최종 시간값 계산·저장
  silver2/
    speed_segment.py        # 신규(재설계): link_points 재투영 + LION geometry buffer/STRtree 매칭
  lion/
    bronze.py / silver1.py  # 기존 유지
    gold2.py                # 기존 유지, centrality 관련 함수만 제거
                             # (silver2.py는 폐기 대상 — adjacency 그래프 전용)
  serving/
    nav_api.py              # 신규: type 라우팅 + 조회만. 계산 없음. 무결점 응답 강제
  construction/, construction_stipulations/, event/, road_closures/,
  ticketmaster/, taxi_zone/, tlc/, silver2/(zone_segment.py 등)
                             # 그대로 보존, 지금 범위 아님

dags/
  speed_pipeline.py          # 신규: bronze(30분 스케줄) ~ silver1 ~ gold2
  나머지 도메인 DAG           # 그대로 유지 (gold_closure_penalty.py는 삭제)

data/ (또는 RDS 서빙 테이블)
  bronze/speed/dt=.../
  silver1/speed/dt=.../
  silver2/map_speed_segment.parquet
  gold2/dim_segment.parquet                 # lion, type=2 소스
  gold2 RDS 테이블: gold_nav_type_1_time(segment_id, date, time_slot, value)

# 삭제: dashboard/, src/serving/api.py, config/traffic_score_weights.yaml,
#       docs/traffic_score_methodology.md
```

## 브랜치 전략

`main`과 `develop`이 이미 갈라져 있고(확인 결과 `main`은 사실상 빈 브랜치 — 실제 코드베이스는
전부 `develop`에만 있음), 실제 개발은 계속 `develop` 기준으로 진행 중이다.

```
develop
  └─ nav                        (통합 브랜치, 접두사 없음)
       ├─ chore/nav-retire-old   # 1번째: 위 "폐기" 목록 삭제부터
       ├─ feature/nav-schema     # 공통 스키마/API 계약/RDS 테이블 정의
       ├─ feature/nav-length     # lion/gold2.py 정리 + type=2 서빙 연결
       ├─ feature/nav-time       # speed 도메인 신설(bronze~gold2) + speed_pipeline DAG
       └─ feature/nav-serving-api # nav_api.py
```

각 서브 브랜치는 `nav`로 PR 머지, 전체 검증 후 `nav` → `develop` 머지. `chore/nav-retire-old`를
맨 먼저 두는 이유는 새로 만들 것 위에 옛 코드를 얹어두지 않고 처음부터 깨끗한 트리에서
시작하기 위함이다.

## 미결정 사항 (구현 계획 단계에서 결정)

1. 날짜+30분 키의 보존/생성 범위 (rolling window 정책 — 과거 며칠 유지, 미래 몇 시간 미리 계산)
2. type=1(시간) 계산에 도로 통제/공사 같은 실시간 신호를 반영할지 여부
3. 무결점 응답의 구체적 fallback 값/로직 (예: road_class 기반 기본 속도)
4. 스케치에 있던 type=3(혼잡도로 추정) 정의
5. 가중치 공식의 최종 형태(선형 vs 지수) — 실측 후 재검토
6. 시간값의 단위(초/분) 확정
