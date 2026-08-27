<h1 align="center">🌐 택시 내비게이션용 도로 정보 API</h1>

<p align="center">
내비게이션 경로 계산에 필요한 도로 세그먼트별 정보(소요시간·길이·통행료 등)를 데이터 파이프라인으로 구축해 API로 제공합니다.<br>
택시 전용 기능으로, 세그먼트별 택시 승차 승객 수 정보도 함께 제공합니다.
</p>

<p align="center">
  <a href="https://nav-api-dashboard-lsy341.s3-website.ap-northeast-2.amazonaws.com"><img src="https://img.shields.io/badge/대시보드_바로가기-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="대시보드"/></a>
  <a href="http://3.38.96.76:8080"><img src="https://img.shields.io/badge/Airflow_바로가기-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/></a>
  <a href="http://3.38.96.76:3000/d/nav-gold-overview/nav-gold-overview-rds-2b-emr-serverless?from=now-12h&to=now&timezone=browser&refresh=5m"><img src="https://img.shields.io/badge/Grafana_바로가기-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana"/></a>
</p>

<p align="center">
<sub>소프티어 부트캠프 8기 · Data Engineering 5조 · 김지원 · 이동찬 · 이승연</sub>
</p>

---

## 목차

1. [프로덕트 개요](#1-프로덕트-개요)
2. [최종 데이터 스키마](#2-최종-데이터-스키마)
3. [데이터 파이프라인과 아키텍처](#3-데이터-파이프라인과-아키텍처)
4. [타입별 설계: 데이터가 다르면 답도 다르다](#4-타입별-설계-데이터가-다르면-답도-다르다)
5. [무조건 응답하는 서비스 만들기](#5-무조건-응답하는-서비스-만들기)
6. [운영과 성능](#6-운영과-성능)
7. [기술적 고민과 결정](#7-기술적-고민과-결정)
8. [기술 스택](#8-기술-스택)
9. [한계와 다음 단계](#9-한계와-다음-단계)
10. [팀원 소개](#10-팀원-소개)

## 1. 프로덕트 개요

### **문제 정의**
- **누구의**: 택시 내비게이션 개발 회사의 라우팅 엔지니어링팀

- **어떤 문제**: 라우팅 알고리즘을 갖고 있지만, 도로별 최신 데이터(통행 소요시간, 길이, 통행료 등) 및 승차 승객 수 데이터가 경로 계산에 바로 활용 가능한 형태로 제공되지 않습니다.

- **해결책**: 택시 내비게이션 경로 탐색에 필요한 도로 세그먼트(10-20m 수준의 도로 조각)별 정보를 데이터 파이프라인으로 구축해 API로 제공합니다.

### **주요 기능**

| type | 지표 | 용도 |
| :---: | --- | --- |
| 1 | 도로 세그먼트별 통행 소요시간 | 빠른 경로 | 
| 2 | 도로 세그먼트별 길이 | 짧은 경로 |
| 3 | 도로 세그먼트별 택시 승차 승객 수 (택시 전용) | 승객 많은 경로 | 
| 4 | 도로 세그먼트별 통행료 | 무료 경로 |

---

## 2. 최종 데이터 스키마

4개 지표는 갱신 주기와 grain이 달라 테이블을 4개로 분리했습니다. 조회 시 추가 쿼리 없이 한 번에 필요한 값을 다 가져올 수 있도록, 관련된 값은 같은 행의 컬럼으로 둡니다(PK: **segment_id, time**).

### **segment_metrics_type1: 통행 소요시간**

| 컬럼 | 설명 | 예시 |
| --- | --- | --- |
| segment_id | 세그먼트 식별자 | "0151677" |
| time | 30분 단위 시간 버킷 | "0830" |
| value | 최신 실측 통과시간(초) | 38 |
| last_sample_at | value가 관측된 시각 — 신선도 판정용 | 2026-08-27T08:31:02Z |
| avg | 과거 평균 통과시간(초) | 44 |
| count | avg 증분 갱신 계산용 누적 횟수 | 312 |
| updated_date | 이 행이 마지막으로 갱신된 날짜 | 2026-08-27 |

### **segment_metrics_type2: 길이**

| 컬럼 | 설명 | 예시 |
| --- | --- | --- |
| segment_id | 세그먼트 식별자 | "0151677" |
| value | 길이(m) | 255 |

### **segment_metrics_type3: 승차 승객수**

| 컬럼 | 설명 | 예시 |
| --- | --- | --- |
| segment_id | 세그먼트 식별자 | "0151677" |
| dow | 요일 | "MON" |
| time | 30분 단위 시간 버킷 | "0900" |
| value | 평균 승차 수 | 12 |

### **segment_metrics_type4: 통행료**

| 컬럼 | 설명 | 예시 |
| --- | --- | --- |
| segment_id | 세그먼트 식별자 | "0247694" |
| value | 혼잡통행료 + 도로통행료 합산 금액(달러) | 0.75 |

### **API 요청-응답 예시**

| type | 예시 요청 | 예시 응답 | 소스 |
| :---: | --- | --- | --- |
| 1 (통행 소요시간) | `{"segment_ids": ["1000", "1001", "1002"], "type": 1, "date": "2026-08-27", "time": "09:00"}` | `{"value": [38, 23, 24]}` — 초 | [도로 속도 관측 데이터(5분 주기)](https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9) |
| 2 (길이) | `{"segment_ids": ["1000", "1001", "1002"], "type": 2, "date": "2026-08-27", "time": "09:00"}` | `{"value": [25, 32, 20]}` — m | [NYC 도로망(LION) 원본](https://data.cityofnewyork.us/City-Government/LION/2v4z-66xt) |
| 3 (택시 승차 승객 수) | `{"segment_ids": ["1000", "1001", "1002"], "type": 3, "date": "2026-08-27", "time": "09:00"}` | `{"value": [12, 20, 12]}` — 승객 수 | [NYC TLC 택시 운행 기록](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| 4 (통행료) | `{"segment_ids": ["1000", "1001", "1002"], "type": 4, "date": "2026-08-27", "time": "09:00"}` | `{"value": [0.75, 0, 0]}` — 달러 | [MTA·Port Authority 통행료 데이터](https://www.mta.info/fares-tolls/tolls/vehicle-types) |

라우팅 엔지니어링팀은 API로 받은 세그먼트별 데이터를 자체 라우팅 알고리즘에 결합해, 최종 고객에게 아래 4가지 종류의 경로를 제공할 수 있습니다.

**1. <mark style="background-color:#fef08a; color:#1a1a1a;">빠른 경로</mark>**

**2. <mark style="background-color:#fef08a; color:#1a1a1a;">최단 거리 경로</mark>**

**3. <mark style="background-color:#fef08a; color:#1a1a1a;">무료 경로</mark>**

**4. <mark style="background-color:#fef08a; color:#1a1a1a;">승객 많은 경로</mark>**

## 3. 데이터 파이프라인과 아키텍처

**INPUT**

| 제공처 | 수집 대상 | 수집 방식 · 주기 |
| --- | --- | --- |
| NYC DOT / NYC Open Data | 도로별 속도 데이터 | Socrata API · 5분 |
| NYC DCP / NYC Open Data | 도로망(LION), 세그먼트 약 10만 개 | Socrata API · 분기 1회 |
| NYC TLC Data | 택시 운행 기록 | 정적 파일 다운로드 · 월 1회 |
| NYC TLC Data | 택시존, 263개 zone | 정적 파일 다운로드 · 최초 1회 |
| MTA·Port Authority / NY Open Data | 도로·혼잡 통행료 | 크롤러 · 정책 변경 시 |

**OUTPUT**

| type | 계산 방식 | 산출값 |
| :---: | --- | --- |
| 세그먼트별 통과시간 (type1) | 길이 ÷ 가중평균 속도 | 30분 버킷 통과시간(초) |
| 세그먼트별 길이 (type2) | LION 원본 그대로 | 정적 길이값(m) |
| 세그먼트별 택시 승차수요 (type3) | 최근 N주 rolling 평균 → zone→segment 확산 | 요일×30분 슬롯 평균 승차수 |
| 세그먼트별 통행료 (type4) | 혼잡통행료 + 도로통행료 합산 | 세그먼트당 통행료 |

**아키텍처**

<p align="center">
  <img src="🚧 TODO: 아키텍처 다이어그램 이미지 URL" width="100%" alt="시스템 아키텍처">
</p>

| 단계 | 구성 | 내용 |
| --- | --- | --- |
| Bronze | S3 | 원본 그대로 저장 |
| Silver1/2 | S3 · Spark | 정제 + 도로망 매핑/조인 |
| Gold | PostgreSQL · Spark | type1~4 최종 지표 upsert |
| Data Access | EC2 · FastAPI | 도로별 정보 조회 API |

> ⚠️ `nav` 코드는 아직 Lambda + DynamoDB 기준입니다. 위 구성은 이번에 확정한 목표 아키텍처(EC2+FastAPI+RDS)입니다.

## 4. 타입별 설계: 데이터가 다르면 답도 다르다

4개 타입은 같은 파이프라인을 거치지만, 원본 데이터의 갱신 주기·정밀도·노이즈 특성이 서로 달라 타입마다 다른 설계 결정을 내렸습니다.

### Type1 — 세그먼트별 통행 소요시간

원본(도로 속도 데이터)은 5분마다 갱신되지만 결측이 많아 fresh한 값을 항상 확보할 수는 없습니다. 1-2시간만 오래돼도 실제 교통 상황과 어긋날 만큼 신선도가 중요해, 결측 시에도 정확한 대체값이 필요합니다. 다만 그 대체값을 구하는 연산이 무거워지면 파이프라인이 5분 주기를 따라잡지 못하므로, 정확도와 파이프라인 처리 속도를 동시에 지켜야 했습니다.

RDS는 정상 응답했지만 이 세그먼트·시간대 값의 신뢰도가 낮을 때(결측/오래됨), 신뢰도 순으로 내려가는 3단계 체인을 둡니다.

| 단계 | 값 | 조건 |
| :---: | --- | --- |
| 1 | 최신 실측값 (Fresh) | `현재 - last_sample_at ≤ 신선도 기준` |
| 2 | 과거 대표값 (Historical AVG) | Fresh 없거나 오래됨 |
| 3 | 코드 기본값 (Hardcoded) | AVG도 없음 |

AVG는 매번 전체 히스토리를 재계산하지 않고, 새 관측값이 들어올 때마다 기존 평균을 살짝 업데이트하는 지수이동평균(EMA) 방식으로 계산합니다.

```
new_avg = old_avg + (new_value - old_avg) / min(count, 스무딩 윈도우)
```

`count`는 이 슬롯이 몇 번 갱신됐는지 추적해 최근 값 반영 비중을 조절합니다 — 초반엔 안정적으로 쌓이다가, 일정 횟수를 넘으면 최근 값에 더 큰 비중을 줘 도로공사 등 실제 패턴 변화를 빠르게 따라잡습니다. 매번 전체 히스토리를 훑지 않으니 연산량도 늘지 않습니다.

또한 경로 전체를 요청 시각 하나로 조회하지 않고, 앞선 세그먼트들의 예상 소요시간을 누적해 각 세그먼트의 예상 진입 시각을 계산한 뒤 그 시간대 값을 조회합니다. 예상 진입 시각이 미래라면 그 시간대 실측값은 아직 존재할 수 없으므로, Fresh 대신 Historical AVG를 곧바로 사용합니다.

### Type2 — 세그먼트별 길이

값 자체는 정적이지만, LION 도로망은 Type1·Type3가 세그먼트 정의의 기준으로 삼는 데이터라 다른 타입에 영향이 전파됩니다. 매핑 결과를 콘텐츠 해시로 버전 관리해, 하위 파이프라인이 자신이 사용한 매핑 버전을 함께 기록하고 최신 버전과 비교하도록 설계했습니다. 도로망이 갱신되더라도 하위 파이프라인이 조용히 낡은 매핑으로 남지 않고 자동으로 재계산되는 구조입니다.

### Type3 — 세그먼트별 택시 승차 승객 수

TLC 원본 데이터는 픽업 위치가 zone(구역) 단위로만 기록돼 세그먼트별로 직접 집계할 수 없습니다. zone 평균을 계산한 뒤 해당 zone의 모든 세그먼트에 동일하게 복제해 서빙합니다. 이 복제 과정에서 결과 건수가 요일·시간대까지 곱해져 7,300만 건까지 불어나는데, zone당 세그먼트 수가 22개~3,435개로 편차가 커 zone 단위로 처리하면 특정 파티션에 데이터가 몰리는 스큐가 발생해 OOM 장애로 이어졌습니다. 대신 훨씬 촘촘한 segment_id 기준으로 재분배해 스큐를 해소했습니다(파티션 최대·최소 크기 비율 105배→1.2배).

### Type4 — 세그먼트별 통행료

대부분의 세그먼트가 통행료 대상이 아니라 값이 극도로 희소하고, 정책 변경 시에만 갱신되는 정적 데이터입니다. RDS가 정상 응답했는데 특정 세그먼트가 없는 것은 장애가 아니라 "진짜로 통행료가 없는 도로"라는 정상 응답으로 구분해, RDS 조회 실패(연결 불가)와 값 부재를 서로 다른 상황으로 취급합니다.

> 테이블 스키마는 [2. 최종 데이터 스키마](#2-최종-데이터-스키마)를 참고하세요.

## 5. 무조건 응답하는 서비스 만들기

내비게이션 경로 계산은 여러 세그먼트 값을 실시간으로 조합해야 해서, 값 하나가 지연되면 경로 계산 전체가 막힙니다. 그래서 "정확한 값을 오래 기다리기"보다 "짧은 시간 안에 항상 유효한 값을 반환"하는 걸 최우선 목표로 삼았습니다 — 장애 상황에서도 API가 에러·무응답을 내지 않고, 응답 지연 없이, 다소 오래된 값이라도 즉시 돌려줍니다.

**왜 RDS인가** — 조회 패턴이 key-value 수준으로 단순하고 데이터 규모도 DynamoDB의 대규모 수평 확장이 필요한 수준은 아니라고 판단해, 관리형 멀티 AZ 가용성을 제공하는 DynamoDB 대신 비용 효율적인 RDS(PostgreSQL)를 Primary Serving Store로 선택했습니다. 대신 DynamoDB가 인프라 레벨에서 제공하던 가용성을, 애플리케이션 레벨의 대체 경로로 재현합니다.

**인프라 장애 시 대체 경로** — RDS 자체가 응답 불가능하면(연결 실패, 타임아웃) Lambda 메모리 캐시 → S3 Gold 스냅샷(마지막 정상 데이터) → 코드 하드코딩 기본값 순으로 내려갑니다. 연결·쿼리 타임아웃을 1초로 짧게 걸고 재시도는 하지 않아, 장애 상황에서 응답이 지연되는 대신 곧바로 다음 단계로 넘어갑니다.

**실제로 검증된 사례** — Type3 배치(Gold 갱신) 파이프라인이 서빙 테이블에 대량 upsert를 하는 동안, 조회 쿼리가 그 쓰기 락에 걸려 statement_timeout(1초)으로 계속 취소되는 장애를 실제로 겪었습니다. 하드코딩 폴백 비율이 평소 대비 93.5%까지 치솟았고, RDS CPU(30%대)·디스크 지연(10~20ms)은 정상이었지만 DatabaseConnections만 57→76으로 튀어 있어 원인이 디스크·CPU가 아니라 쓰기 트랜잭션의 락 대기라는 걸 확인했습니다.

해결책은 행 단위 upsert 대신, 새 데이터를 별도 테이블에 완전히 채운 뒤 테이블 이름만 원자적으로 스왑(RENAME)하는 방식입니다. 인덱스도 데이터를 다 채운 뒤 한 번에 생성합니다 — 처음엔 인덱스가 있는 빈 테이블에 채워 넣다가 행마다 B-tree를 갱신하느라 20분 넘게 끝나지 않는 사고가 있었고, 정렬 후 한 번에 인덱스를 쌓는 방식(`ADD PRIMARY KEY`)으로 바꿔 해결했습니다. RENAME은 메타데이터만 바꾸는 작업이라 밀리초 안에 끝나, 갱신 중에도 조회가 막히지 않습니다.

## 6. 운영과 성능

- **데이터 품질 검증(Great Expectations)** — Bronze 적재 시 taxi_type별 필수 컬럼이 다 있는지 검증합니다. Silver1 변환 로직과 완전히 같은 컬럼 매핑을 공유해, 검증 기준과 실제 변환 로직이 서로 어긋나는 걸 방지합니다. 검증 실패 시 해당 파일은 처리에서 제외됩니다.
- **삭제된 세그먼트 정리** — LION 도로망이 갱신돼 사라진 세그먼트는, 최신 유효 세그먼트 집합에 없는 기존 행을 안티조인으로 찾아 자동 삭제합니다. 유효 집합이 비어 있으면 상류 버그로 보고 삭제 대신 예외를 던져, 테이블 전체가 실수로 비워지는 걸 막습니다.
- **재실행/중복 방지** — Gold 적재는 `(segment_id, sk)` upsert. type3는 워터마크(기간·매핑버전)로 재계산 필요 여부만 판단합니다.
- **모니터링(Grafana)** — RDS 쿼리 응답시간(p50/p95/p99), 타입별 fallback 계층 비율, API 전체 응답시간을 봅니다. fallback 비율은 사용자가 실제로 얼마나 자주 부정확한 값을 받는지 보여줘 개선 우선순위를 정하는 근거가 되고, API·RDS·Lambda 응답시간을 나란히 보면 병목이 RDS인지 Lambda 콜드스타트인지 구분할 수 있습니다.
- **S3 Staging Lifecycle** — 실패로 남은 임시 결과만 7일 뒤 자동 삭제(`config/s3-staging-lifecycle.json`).
- **실제 이슈 대응** — EC2 CPU 경합으로 Airflow DagBag import timeout 발생 → 120초로 조정. 프로토타입 단계 DynamoDB 32-way 쓰기 병렬이 처리량 한도 초과 → 10-way + adaptive 재시도로 해결.

## 7. 기술적 고민과 결정

각 결정의 배경(관측된 사실, 고려한 대안, 기각 사유, 검증 방법)은 링크한 문서에서 자세히 볼 수 있습니다.

| 고민 | 결정 | 링크 |
| --- | --- | --- |
| (예시) 서빙 저장소로 무엇을 쓸까 → | (예시) 비용 이슈로 DynamoDB 대신 RDS(PostgreSQL) 선택 | [상세](docs/decisions/00-example.md) |
|  |  | [상세](docs/decisions/01-decision.md) |
|  |  | [상세](docs/decisions/02-decision.md) |
|  |  | [상세](docs/decisions/03-decision.md) |
|  |  | [상세](docs/decisions/04-decision.md) |
|  |  | [상세](docs/decisions/05-decision.md) |
|  |  | [상세](docs/decisions/06-decision.md) |
|  |  | [상세](docs/decisions/07-decision.md) |
|  |  | [상세](docs/decisions/08-decision.md) |
|  |  | [상세](docs/decisions/09-decision.md) |
|  |  | [상세](docs/decisions/10-decision.md) |
|  |  | [상세](docs/decisions/11-decision.md) |
|  |  | [상세](docs/decisions/12-decision.md) |
|  |  | [상세](docs/decisions/13-decision.md) |

## 8. 기술 스택

| 영역 | 스택 |
| --- | --- |
| **오케스트레이션** | ![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white) |
| **대용량 처리** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![AWS EMR Serverless](https://img.shields.io/badge/EMR%20Serverless-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white) |
| **데이터 품질** | ![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6310?style=for-the-badge&logo=greatexpectations&logoColor=white) |
| **저장소** | ![S3](https://img.shields.io/badge/S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white) ![Amazon RDS](https://img.shields.io/badge/RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white) |
| **서빙 API** | ![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white) |
| **인프라** | ![EC2](https://img.shields.io/badge/EC2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white) |
| **CI/CD** | ![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white) |
| **모니터링** | ![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white) |

> DynamoDB도 검토했지만 비용 이슈로 RDS(PostgreSQL)를 최종 채택 ([5. 무조건 응답하는 서비스 만들기](#5-무조건-응답하는-서비스-만들기)).

## 9. 한계와 다음 단계

- Type3는 zone 평균값을 그 zone에 속한 모든 세그먼트에 동일하게 적용합니다 — 세그먼트별 가중치(도로 유형, 위치 등)를 반영해 차등화하는 방안을 검토 중입니다.
- RDS 전환 후 실제 장애 상황의 응답 지연·성공률은 별도 측정이 필요합니다.
- Type3(택시 승차 수)은 실제 확률이 아닌 과거 평균 근사치 — 날씨·이벤트 변수로 확장 가능합니다.
- 신선도(freshness) 임계값이 현재 고정값 — 세그먼트별 동적 임계값으로 개선할 수 있습니다.
- Gold 파이프라인이 아직 끝나지 않은 시점에 요청이 들어오면 어떤 값을 반환해야 하는지는 아직 결론을 내리지 못했습니다.
- EMR 작업을 여러 개로 나눠 돌리는 대신 한 번에 묶어 돌리는 방안과, 데이터 유입 자체를 확인하는 전용 대시보드를 검토 중입니다.
- RDS 커넥션 수를 점진적으로 줄여가며 병목이 어디서 생기는지 실험적으로 확인하는 작업이 남아 있습니다.

## 10. 팀원 소개

<div align="center">

<table>
  <tr>
    <td align="center"><a href="https://github.com/dongchan21"><img src="https://github.com/dongchan21.png" width="120px" alt="이동찬"/></a></td>
    <td align="center"><a href="https://github.com/lsy341"><img src="https://github.com/lsy341.png" width="120px" alt="이승연"/></a></td>
    <td align="center"><a href="https://github.com/tmsklo0428"><img src="https://github.com/tmsklo0428.png" width="120px" alt="김지원"/></a></td>
  </tr>
  <tr>
    <td align="center"><b><a href="https://github.com/dongchan21">이동찬</a></b></td>
    <td align="center"><b><a href="https://github.com/lsy341">이승연</a></b></td>
    <td align="center"><b><a href="https://github.com/tmsklo0428">김지원</a></b></td>
  </tr>
</table>

</div>
