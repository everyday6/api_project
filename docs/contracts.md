# 데이터 계약 (Data Contracts)

> 이 문서는 서빙 테이블 4개(`segment_metrics_type1~4`)와 API 응답의 계약을
> 명시한다. README의 스키마 표를 대체하지 않고, 거기 없던 **null 허용
> 여부·필수 여부·tier 어휘**를 추가로 명시한다. 코드가 이 계약과 달라지면
> 코드가 아니라 이 문서를 먼저 고친다 — 계약을 코드보다 먼저 결정한다는
> 원칙(`RELIABILITY_PRINCIPLES.md` "처음 잡을 때 할 일" 2번)에 따른다.

---

## segment_metrics_type1 (소요시간)

PK: `(segment_id, time)`

| 컬럼 | 타입 | 필수 | Null 허용 | 설명 |
| --- | --- | --- | --- | --- |
| segment_id | string | ✅ | ❌ | 세그먼트 식별자 |
| time | string ("HHMM") | ✅ | ❌ | 30분 단위 시간 버킷 |
| value | int (초) | ✅ | ❌ | 최신 실측 통과시간 |
| last_sample_at | timestamp | ✅ | ❌ | value 관측 시각 — 신선도 판정에 씀 |
| avg | int (초) | ✅ | ❌ | 과거 평균 통과시간 |
| count | int | ✅ | ❌ | avg 증분 갱신용 누적 횟수 |
| updated_date | date | ✅ | ❌ | 이 행이 마지막으로 갱신된 날짜 |

## segment_metrics_type2 (길이)

PK: `segment_id`

| 컬럼 | 타입 | 필수 | Null 허용 | 설명 |
| --- | --- | --- | --- | --- |
| segment_id | string | ✅ | ❌ | 세그먼트 식별자 (`GLOBAL`은 예약된 기본값 파티션 키) |
| value | int (m) | ✅ | ❌ | 길이 |

## segment_metrics_type3 (승차 승객수)

PK: `(segment_id, dow, time)`

| 컬럼 | 타입 | 필수 | Null 허용 | 설명 |
| --- | --- | --- | --- | --- |
| segment_id | string | ✅ | ❌ | 세그먼트 식별자 |
| dow | string ("MON"~"SUN") | ✅ | ❌ | 요일 |
| time | string ("HHMM") | ✅ | ❌ | 30분 단위 시간 버킷 |
| value | float | ✅ | ❌ | 평균 승차 수 |

## segment_metrics_type4 (통행료)

PK: `segment_id`

| 컬럼 | 타입 | 필수 | Null 허용 | 설명 |
| --- | --- | --- | --- | --- |
| segment_id | string | ✅ | ❌ | 세그먼트 식별자 (통행료 대상만 — 희소) |
| value | float (달러) | ✅ | ❌ | 혼잡통행료 + 도로통행료 합산 |

---

## API 응답 — `sources` / tier 어휘

> **중요**: 아래 4개 세트는 서로 다른 어휘다. 한 응답 안에 섞이지 않는다 —
> 어떤 세트가 나올지는 요청의 `type` 하나로 결정된다.

| type | 지표 | 가능한 tier 값 | 의미 |
| --- | --- | --- | --- |
| 1 | 소요시간 | `fresh` | 오늘 실측값 사용 |
| | | `avg` | 오늘 실측 없어 과거 평균 사용 |
| | | `hardcoded` | RDS 응답 자체는 왔으나 슬롯이 없어 코드 상수 사용 |
| 2 | 길이 | `rds` | RDS에서 해당 세그먼트 값을 직접 찾음 |
| | | `global` | RDS는 정상이나 이 세그먼트 값이 없어 전체 기본값(GLOBAL) 사용 |
| | | `snapshot` | RDS 자체가 응답 불가 — S3 스냅샷의 세그먼트별 값 사용 |
| | | `hardcoded` | 스냅샷에도 없어 코드 상수 사용 |
| 3 | 승차 승객수 | `rds` | RDS에서 직접 찾음 |
| | | `snapshot` | RDS 장애 — S3 스냅샷 사용 |
| | | `hardcoded` | 스냅샷에도 없어 코드 상수 사용 |
| 4 | 통행료 | `rds` | RDS에서 직접 찾음 |
| | | `snapshot` | RDS 장애 — S3 스냅샷 사용 |
| | | `hardcoded` | 스냅샷에도 없어 코드 상수 사용 |

### 신뢰도 등급 (참고용 매핑)

클라이언트가 tier를 일일이 알 필요 없이 대략적인 신뢰도만 보고 싶을 때
쓸 수 있는 참고 구분이다 (API가 직접 내려주는 필드는 아니다):

- **높음**: `fresh`, `rds`
- **중간**: `avg`, `global`, `snapshot`
- **낮음**: `hardcoded`

### type별 데이터 성격 — 3버킷

tier 어휘를 다시 보면, 4개 type이 실제로는 성격이 다른 3개 그룹으로
나뉜다(`RELIABILITY_PRINCIPLES.md` Tier 0-B 참고):

| 그룹 | type | 최신성 축 노출? | 비고 |
| --- | --- | --- | --- |
| 드리프트하는 집계 | 1(소요시간) | ✅ `fresh`/`avg` | 최신성이 곧 정확성의 프록시 |
| | 3(수요) | ❌ 없음 | TLC 이력 기반 통계 집계라 수요 자체는 드리프트하지만, 지금 tier 어휘가 그 축을 안 보여준다 — **설계 빈틈**(열린 질문 참고) |
| 정적 기준 파생물 | 2(길이) | ❌ 불필요 | 도로 물리 속성, LION 갱신 전엔 안 바뀜 |
| | 4(통행료) | ❌ 불필요 | 정책 데이터, 불연속적으로 드물게 변경 |

"인프라 가용성" 축(`rds`가 살아있는가/죽었는가 → `snapshot`/`hardcoded`로
떨어지는가)은 4개 type 전부에 있고, 위 "최신성 축"과는 별개다 — 서로
직교한다. 예를 들어 Type2의 `global`은 "RDS는 살아있지만 이 세그먼트 값이
없다"는 뜻으로, 가용성 축의 중간 단계이지 최신성과는 무관하다.

---

## `/segments/values` (type 1, 2) vs `/api/navigation/values` (type 1~4)

- 둘 다 `sources` 필드로 tier를 노출한다 (2026-09 기준 적용됨).
- 응답 필드 이름만 다르다: `/segments/values`는 `values`, `/api/navigation/values`는
  `value`(단수, 기존 계약 유지). tier는 둘 다 `sources`.
- `/api/navigation/values`의 tier 어휘는 `type`에 따라 다르다 (위 표):
  type1 = `fresh`/`avg`/`hardcoded`, type2 = `rds`/`global`/`snapshot`/`hardcoded`,
  type3·type4 = `rds`/`snapshot`/`hardcoded`. 한 응답 안에 한 세트만 나온다.

---

## 이 문서를 갱신하는 규칙

- 새 tier 이름을 코드에 추가하면(`src/common/tier_metrics.py` 호출부),
  같은 PR에서 이 문서의 표도 함께 갱신한다.
- 컬럼을 추가/삭제/타입 변경하면, 코드 변경 전에 이 문서를 먼저 고치고
  리뷰를 받는다.
- README의 스키마 표와 이 문서가 어긋나면 이 문서를 기준으로 README를
  고친다 (이 문서가 계약의 단일 진실 공급원).
