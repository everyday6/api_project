# 신뢰성 원칙 (Reliability Principles) v2

> 이전 버전은 기존 `docs/decisions/`에서 원칙을 역추출한 것이었다. 이 버전은
> 거기에 더해, **원칙들 사이의 우선순위와 그 이유**까지 명시한다. 프로젝트를
> 처음부터 다시 짠다면 이 순서로 잡는 것을 권장한다.

---

## 0. 하나의 공리에서 모든 원칙을 도출한다

> **"이 값이 틀렸다는 걸 누군가 발견했을 때, 우리는 그 사실을 알 수 있고,
> 원인을 추적할 수 있고, 영향 범위를 격리할 수 있는가?"**

가용성이냐 정합성이냐를 먼저 정하지 않는다. 이 질문 하나를 통과시키는 것을
목표로 삼으면, 나머지 원칙은 자연스럽게 우선순위가 정해진다.

### 0-1. "틀린 값"과 "신뢰도가 낮은 값"을 구분한다

데이터 엔지니어링에는 "잘못된 데이터는 없는 데이터보다 못하다"는 격언이
있다. 이 원칙이 실제로 금지하는 것은 **"근사값을 쓰는 것"이 아니라 "근사값을
정상값처럼 위장해서 제공하는 것"** 이다.

- ❌ 틀린 값: 계산 로직 버그, 잘못된 조인, 깨진 스키마로 만들어진, 근거 없이
  정상처럼 보이는 값
- ✅ 신뢰도가 낮은 값: 최신 실측이 없어 과거 평균/캐시/스냅샷을 쓴 값 —
  **단, 그 사실이 응답에 명시되어 있을 때만** 허용된다

우리는 가용성을 우선한다. 이 선택 자체는 문제가 아니다. 문제가 되는 유일한
경우는 **낮은 신뢰도의 값을 신뢰도 표시 없이 내보내는 것**이다.

---

## Tier 구조

### Tier 0 — 트레이드오프를 의식적으로 선택했다고 먼저 선언한다

우선순위는 데이터 성격에 따라 갈린다 — 하나로 뭉뚱그리지 않는다.

**A. 기준 데이터** (Zone-Segment 매핑, LION 도로망, 통행료 정책)

> 정합성 → 가용성 → 응답속도 → 최신성 → 비용

한번 잘못 게시되면 여기 의존하는 모든 실시간 값이 같이 오염된다. 그래서
정합성이 흔들리면(커버리지·참조무결성 검증 실패) 게시 자체를 막는 게 맞고
(`validate_dim_segment_base`의 critical assert가 실제로 이렇게 동작한다),
응답속도·최신성은 상대적으로 덜 급하다 — 몇 시간 늦게 반영돼도 서비스에
큰 타격이 없다.

**B. 실시간 서빙 값** (Type1 소요시간, Type3 수요, 통행료 조회)

> 가용성 → 최신성 → 정확성 → 응답속도 → 비용

매 요청마다 조회되는 값이라 "응답 없음"이 최악의 결과다. 이건 추측이 아니라
[`05-rds-fallback`](docs/decisions/05-rds-fallback.md)에서 실측한
93.58% fallback 사태를 근거로 검증된 선택이다.

> **B는 동질적이지 않다.** `docs/contracts.md`의 tier 어휘를 보면, "최신성"
> 축(`fresh` vs `avg`)이 실제로 노출되는 건 Type1뿐이다. Type2(길이)·
> Type4(통행료)는 도로 물리 속성/정책 데이터라 사실상 A의 파생물이
> 서빙 경로에 얹힌 것에 가깝다(둘 다 tier 어휘가 `rds/snapshot(/global)/
> hardcoded`뿐 — 인프라 가용성 축만 있고 최신성 축이 없다). Type3(수요)는
> TLC 이력 기반 통계 집계라 정적이진 않지만(수요는 계절성 등으로
> 드리프트한다), 현재 tier 어휘가 그 축을 노출하지 않는다 — 이건 정적이라서가
> 아니라 **설계의 빈틈**으로 남겨둔다(아래 "아직 원칙화되지 않은 열린
> 질문" 참고).
>
> "최신성이 정확성의 프록시"라는 명제는 **Type1 한정**이다 — 오늘 실측이
> 있으면 fresh, 없으면 과거 평균(avg)으로 떨어지는 구조라 최신성과 정확성이
> 사실상 한 축으로 묶인다. Type3처럼 최신성 축 자체가 안 보이는 값에는 이
> 명제가 적용되지 않는다.

> 이 선언이 먼저 나와야, 이후의 모든 결정이 "어쩌다 보니 이렇게 됐다"가
> 아니라 "근거를 갖고 선택했다"로 읽힌다.

### Tier 1 — Detectability: 알 수 있는가 (최우선 실행 순위)

품질 문제는 발견하지 못하면 존재하지 않는 것처럼 취급된다.

| # | 원칙 | 지금 상태 | 최소 조치 |
| --- | --- | --- | --- |
| 1 | **Lineage/Reproducibility** | 🟡 type1은 적용(2026-09), type3는 후속 | `segment_metrics_type1`에 `avg_formula_version` 추가 — `src/common/provenance.py`의 `formula_version("v1", 핵심상수)` = "라벨+해시". 라벨은 로직 구조 변경 시 수동, 해시는 스무딩 윈도우가 바뀌면 자동으로 달라져 코드와 조용히 어긋나지 않는다. type3 rolling 평균도 같은 스탬프 예정 |
| 2 | **Contract** | 🟡 README 표만 존재 | `docs/contracts.md`로 분리, null 허용/필수 여부 명시 |
| 3 | **Observability (데이터 상태)** | ✅ 강함 — 유지·어필 | RDS 쓰기중 읽기지연을 실측으로 잡아낸 사례를 계속 근거로 사용 |
| 4 | **응답의 신뢰도 노출** | ✅ `/segments/values`·`/api/navigation/values` 둘 다 적용 완료(2026-09) | `sources` 필드로 값의 출처 계층을 노출 — **원칙 0-1을 지키는 핵심 조치**. type별 tier 어휘는 `docs/contracts.md` 참고 |

### Tier 2 — Containment: 격리할 수 있는가

문제를 발견했다면, 다음은 퍼지지 않게 막는 것이다.

| # | 원칙 | 지금 상태 | 최소 조치 |
| --- | --- | --- | --- |
| 5 | **Quarantine** | 🟡 **도메인마다 다름** (아래 "GX 적용 현황" 참고) | 6개 데이터 생산 도메인 전부 저장 전 크리티컬 게이트 확보. `speed`/`tlc`/`lion`/`silver2`는 `is_suspect` 표시까지, `taxi_zone`/`toll`은 크리티컬 게이트만 |
| 6 | **Idempotency** | ✅ 강함 | `06-etag-marker`, `04-rds-insert` 유지 |
| 7 | **재현성 (Reproducibility)** | 🟡 감사 완료(2026-09) — 아래 참고 | "Bronze가 immutable한가"(체크박스)가 아니라 **"고정된 Bronze 상태에서 Gold를 재도출할 수 있는가"**로 재정의. DELETE는 전무. 시계열 소스(tlc/speed)는 append-only. 갭이던 `taxi_zone` shapefile을 `lion`과 같은 `version_date=` 파티션으로 통일 |

#### GX(품질 검증) 적용 현황 — 2026-09 코드 감사 결과

전체 8개 도메인(`lion`, `nav_length`, `nav_time`, `taxi_zone`, `toll`,
`silver2`, `speed`, `tlc`) 중 `nav_length`/`nav_time`을 뺀 6개 데이터
생산 도메인은 모두 최소한 크리티컬 게이트(저장 전 검증)를 갖췄다.

| 도메인 | 검증 실행 | 실패 시 로그 | **검증 결과가 저장 데이터에 남는가** |
| --- | --- | --- | --- |
| `speed` | ✅ GX(critical+log-only) | ✅ | ✅ `is_suspect` 컬럼 + **비율 급증 시 critical 승격**(`suspect_ratio_ok`) (2026-09 적용) |
| `tlc` | ✅ GX(critical+log-only) | ✅ | ✅ `is_suspect` 컬럼 + **비율 초과 시 파일 제외**(`suspect_fraction` in `_validate_bronze`) (2026-09 적용 — `silver1_transform.py`) |
| `lion` | 🟡 critical만 raw assert, log-only는 GX 없음(의도적 — 아래 참고) | ✅ (critical만) | ✅ `is_suspect` 컬럼 + **비율 임계치 게이트** (2026-09 적용 — `silver1.py`) |
| `silver2` (zone_segment) | 🟡 critical은 raw assert(coverage/unique/null/zone_id 범위), log-only는 `nearest` 매핑 표시 | ✅ (critical만) | ✅ `is_suspect` 컬럼 + **`nearest` 비율 게이트** (2026-09 적용 — `zone_segment.py`) |
| `taxi_zone` | 🟡 critical만 raw assert(shapefile feature 수 250~280) | ✅ (critical만) | N/A — Silver1이 shapefile 통째 복사라 표시할 행 자체가 없음 |
| `toll` (silver2) | 🟡 critical만 raw check(빈 결과/segment_id null/중복/미지의 facility_key) | ✅ (critical만) | N/A — (segment_id, facility_key)뿐이라 행 단위 이상치 신호가 약해 `is_suspect` 대상 아님 (2026-09 적용 — `silver2.py`) |

`speed`/`tlc`의 문제는 해소됐다 — 각각 `bronze_validation.py`/
`silver1_transform.py`에 `mark_suspect_rows()`를 추가해 파이프라인이 저장
직전 컬럼을 표시하도록 연결했다. `tlc`는 GX가 검사하는 조건(필수 컬럼
null, location_id 범위, passenger_count/trip_distance 음수)을 taxi_type별
`COLUMN_MAPPING`을 그대로 참조해 재현했고, Gold(`gold2.py`)가 Silver를
명시적 컬럼명으로 읽고 있어 스키마에 컬럼을 추가해도 다운스트림에 영향이
없음을 확인했다.

`lion`은 처음엔 speed/tlc와 같은 GX log-only 목록(`expectations.py`)을
만들었지만, **실제로 GX 엔진에 실행하는 코드가 없어 죽은 코드였다** —
`mark_suspect_rows()`가 그 조건을 손으로 재현해서 돌고 있었을 뿐이다.
GX 객체는 지우고, 다음 판단으로 대체했다:

- LION은 분기 1회 갱신되는 저빈도 릴리즈라 사람이 결과를 들여다볼
  가능성이 높다 — speed(30분마다)처럼 매 사이클을 사람이 못 보는 고빈도
  파이프라인과 달리 "부드러운 경고"(log-only Slack)의 가치가 낮다.
- 대신 `mark_suspect_rows()`가 표시한 `is_suspect` 비율이
  `MAX_SUSPECT_RATIO`를 넘으면 `validate_dim_segment_base()`의 assert가
  publish 자체를 차단한다(A. 기준 데이터 - 정합성 우선 원칙과 일관).
  Airflow task가 실패하면 기존 `on_failure_callback` 경로로 알림이 가서
  알림 체계에 구멍이 생기지 않는다.
- `SPEED_LIMIT_MIN_MPH`/`MAX_MPH` 같은 임계값 상수만 `expectations.py`에
  남겨 `mark_suspect_rows()`와 (향후 필요하면) 다른 검증이 공유한다.

`silver2`(zone_segment)도 같은 판단이다 — 이미 `validate_map_zone_segment()`가
raw assert로 크리티컬 게이트를 하고 있어서, 여기에 `nearest`(중점이 어떤 zone에도
안 들어가 최근접으로 스냅된 저신뢰 매핑) 행을 `is_suspect`로 표시하고, 그 비율이
`MAX_SUSPECT_RATIO`를 넘으면 publish를 차단하는 것만 얹었다. `lion`과 같은
`src/common/suspect.py` 헬퍼를 그대로 쓴다.

각 도메인의 `mark_suspect_rows()`가 반복 구현돼 임계값이 따로 관리되며 어긋날
위험은, 공통 기계 부분(복사본 생성·NA→False 확정·`is_suspect` 컬럼명·비율
계산)을 `src/common/suspect.py`로 모으고, 값 범위 등 도메인별 임계값 상수는
각 도메인 `expectations.py`(tlc는 `silver1_transform.py`, silver2는
`zone_segment.py`)에서 판정 함수가 그대로 참조하는 방식으로 줄였다.

`toll`(silver2)은 마지막까지 검증이 전무했는데, `is_suspect`가 아니라
**크리티컬 게이트**를 붙였다 — 매핑 결과가 (segment_id, facility_key) /
(segment_id)뿐이라 행 단위 "이상치" 신호가 약한 대신, 빈 결과·segment_id
결측·중복·`toll_facilities.yaml`에 없는 `facility_key` 넷 중 하나라도 있으면
Gold 요금 계산이 조용히 틀리므로 저장 전에 막는다. 특히 빈 결과 검사는
`match_lion_cbd` 주석이 경고하는 "좌표계 불일치 시 `gpd.sjoin`이 경고만
내고 조용히 0건 반환" 함정을 잡는다.

이로써 6개 데이터 생산 도메인(`speed`/`tlc`/`lion`/`silver2`/`taxi_zone`/
`toll`)이 모두 최소한 저장 전 크리티컬 게이트를 갖췄다. 남은 정교화는
아래 "열린 질문"에 정리했다(임계값 baseline 실측, `nearest` 거리 임계,
Type3 최신성 축 등).

#### Bronze 재현성 감사 — 2026-09

원래 원칙은 "Bronze 계층에 UPDATE/DELETE가 없는지 감사"였는데, immutability는
목적이 아니라 수단이다. 실제로 원하는 속성은 **"고정된 Bronze 상태로부터
모든 Gold 값을 재도출할 수 있는가"**이고, 이 기준으로 다시 감사했다.

**DELETE**: 코드 전체에 Bronze 데이터를 지우는 경로 없음. `rmtree`는 Silver
`_staging` run 디렉터리만, 다운로드 실패 시 `unlink`는 `TMP_DIR`의 미완성
파일만(Bronze로 승격되기 전).

**UPDATE(in-place overwrite)** — 소스별 판정:

| 소스 | Bronze 경로 | 재현 가능? | 판정 |
| --- | --- | --- | --- |
| `tlc` | `<원본 파일명>`, 있으면 스킵 | ✅ | 그대로 — 재작성 자체가 없음 |
| `speed` | `batch_end=<ts>.parquet` | ✅ | 그대로 — 배치당 새 파일. 재시도는 같은 배치=같은 데이터로 덮음(내용 불변) |
| `lion` | `version_date=<날짜>/` 파티션, ETag 가드 | ✅ | 그대로 — 같은 UTC 날 원본이 두 번 바뀌면 그날 파티션을 덮는 이론적 충돌이 있으나, LION 릴리즈는 분기 1회라 운영상 불가능 + 발생 시 로그로 인지됨. 필요해지면 date 대신 etag로 파티션 |
| `taxi_zone` | ~~`shapefile/` 고정~~ → `version_date=<날짜>/shapefile/` | ✅ | **고침** — `map_zone_segment`(기준 데이터)의 입력인데 이전 경계 스냅샷이 사라졌다. `lion`과 같은 파티션 스킴으로 통일(ETag 가드는 이미 있었고, 변경 보존만 빠져 있었다) |
| `toll` `toll_rates.yaml` / `toll_facilities.yaml` | 고정 경로, 무조건 덮어씀 | ✅ | 그대로 — 원본이 `config/*.yaml`(git 추적)이라 버전 이력이 곧 git 이력. Bronze는 그 materialized copy일 뿐 |
| `toll` `cbd_geofence.geojson` | 고정 경로, 무조건 덮어씀 | 🟡 | **조건부 유예** — URL(Socrata) 소스라 git 이력 없음. 다만 법적 경계라 변경이 거의 없어 파티셔닝은 유예하되, **내용 해시 마커**를 추가해 원본이 바뀌면 감지·경고한다(그 경고가 뜨면 파티셔닝 도입 신호) |

**마커/제어 파일**(`_latest_etag.txt`, `_metadata.txt`, speed marker, cbd 해시
마커)은 데이터가 아니라 파이프라인 상태라 덮어쓰는 게 정상이다.

이 감사는 lineage 3부작의 마지막 조각이다 — `sources`(어느 서빙 경로),
`avg_formula_version`(어느 계산 공식), Bronze 재현성(어느 입력) → 서빙된
어떤 값이든 출처를 답할 수 있다.

### Tier 3 — Resilience: 버틸 수 있는가 (여기서부터 엔지니어링 판단력을 적극 어필)

Tier 1·2가 갖춰진 뒤에 얘기해야, "숨기는 시스템"이 아니라 "투명하게 버티는
시스템"으로 읽힌다.

| # | 원칙 | 근거 |
| --- | --- | --- |
| 8 | **Graceful Degradation** | `05-rds-fallback` — 계층형 폴백 |
| 9 | **자원 경합/스큐 대응** | `01-skew`(파티션 스큐 105배 실측 진단), `03-spark-tuning`(DAG별 자원 고정 배분), `04-rds-insert`(대안 4개 비교 후 선택) |
| 10 | **Circuit Breaker** | 미구현 — fallback 비율이 임계치를 넘으면 알림/차단하는 로직은 다음 과제 |

### Tier 4 — 효율/가치

| # | 원칙 | 지금 상태 |
| --- | --- | --- |
| 11 | 비용 최적화 | ✅ `config/s3-staging-lifecycle.json` |
| 12 | 비즈니스 임팩트 검증 | 미측정 — 향후 과제 |

---

## 왜 이 순서인가

- **품질 검증팀**은 "틀렸을 때 어떻게 아나요 → 어떻게 막나요 → 그래도
  버티나요" 순으로 묻는다. Tier 1→2→3 순서가 이 질문 흐름과 정확히 맞는다.
- **데이터 엔지니어**는 판단력 자체를 본다. Tier 3의 실측 기반 의사결정
  (`01-skew`, `03-spark-tuning`, `04-rds-insert`)은 이미 강점이니, Tier 0
  선언 직후에 근거로 함께 제시한다.
- 가용성 우선이라는 결론은 바뀌지 않는다. 다만 **"가용성부터 말하기
  시작하면 품질을 등한시한 것처럼 보이고, Tier 1부터 말하기 시작하면
  근거 있는 절충으로 보인다"** — 순서가 인상을 결정한다.

---

## 처음 잡을 때 실제로 할 일 (우선순위 순)

1. 테이블 설계 시점에 `version`, `source`, `confidence` 컬럼을 **나중에
   추가하지 말고 스키마에 처음부터 포함**한다
   (2026-09 현황: `source`는 응답 `sources` 필드로, `version`은 type1
   `avg_formula_version`으로 사후 반영 — 처음부터 넣었으면 마이그레이션이
   필요 없었을 자리다. `confidence`는 tier→신뢰도 매핑을 참고용으로만 둠)
2. `docs/contracts.md`를 **코드보다 먼저** 쓴다 — 컬럼 의미를 코드 작성 전에
   팀원끼리 합의
3. GX 등 검증 도구가 잡아낸 이상치를, 로그로만 남기지 말고 **저장되는
   데이터 자체에 표시(`is_suspect` 같은 플래그)하는 인터페이스**를
   `src/common`에 처음부터 정의한다 — 검증 실행과 그 결과의 영속화를
   분리하면(TLC 사례) 검증이 사실상 무의미해진다
4. 그 위에 이미 잘하고 있는 fallback·모니터링(Tier 3)을 얹는다
5. 새 `docs/decisions/*.md`를 쓸 때마다 이 문서의 몇 번 원칙에 해당하는지
   한 줄로 표시해 서사를 계속 연결한다

## 아직 원칙화되지 않은 열린 질문

- ~~`tlc` 도메인의 GX log-only 검증 결과가 로그에만 남고 실제 Silver
  데이터에는 반영되지 않는다~~ → 2026-09 `mark_suspect_rows`로 해소
- ~~행/배치 단위 검증이 일부 도메인에만 있다~~ → 2026-09 기준 6개 데이터
  생산 도메인 전부 저장 전 크리티컬 게이트를 갖췄다(위 GX 적용 현황). 남은
  건 `is_suspect` 커버리지 확대(현재 speed/tlc/lion/silver2)와 임계값
  정교화지, "검증이 아예 없는" 도메인은 없다
- ~~log-only 이상치의 비율이 평소보다 급격히 늘어났을 때도 지금은
  critical로 승격되지 않는다~~ → **네 파이프라인 전부 해소**(2026-09).
  `lion`/`silver2`는 `MAX_SUSPECT_RATIO` publish 게이트, `speed`는
  `suspect_ratio_ok()`(비율 초과 시 이번 사이클 저장 스킵 + Slack), `tlc`는
  `_validate_bronze`가 `suspect_fraction()`으로 파일별 비율을 재서 임계치
  초과 파일을 critical처럼 제외(`spark_jobs/tlc_pipeline_job.py`).
- `lion`(`MAX_SUSPECT_RATIO`=0.05)·`silver2`(=0.10, `nearest` 비율)·
  `speed`(=0.20, 고빈도라 넉넉)·`tlc`(=0.15, 월 단위 파일)의 임계값은
  전부 **placeholder다** —
  실제 스냅샷/배치로 baseline을 측정할 프로덕션 접근 권한이 없어 실측
  없이 박아둔 값이다. 접근 권한이 생기는 대로 재고 조정해야 한다(코드
  주석에도 명시). `silver2`는 추가로 `nearest`여도 `distance_ft`가 작으면
  (zone 경계 바로 옆) 사실상 정상이므로 거리 임계값을 함께 보는 정교화가
  남아 있다
- Type3(수요)의 tier 어휘가 최신성 축을 노출하지 않는다 — Type1처럼
  `fresh`/`avg` 구분을 추가할지, 아니면 정말 노출할 필요가 없는지 판단이
  필요하다(`docs/contracts.md` 참고)
- `avg_formula_version`(lineage)이 type1에만 있다 — type3(수요 rolling
  평균, `src/tlc/gold2.py`)도 계산값이라 같은 `formula_version()` 스탬프를
  붙여야 한다. type2(길이)·type4(통행료)는 조회/패스스루라 대상 아님
- fallback 비율이 임계치를 넘었을 때 자동 알림/circuit breaker로 이어지는
  경로가 없다
- Bronze가 실제로 immutable한지 코드 레벨에서 아직 검증하지 않았다
