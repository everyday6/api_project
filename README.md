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
4. [AWS 아키텍처](#4-aws-아키텍처)
5. [Airflow DAG 설계](#5-airflow-dag-설계)
6. [예외 처리 및 스키마 검증](#6-예외-처리-및-스키마-검증)
7. [모니터링](#7-모니터링)
8. [기술적 고민과 결정](#8-기술적-고민과-결정)
9. [기술 스택](#9-기술-스택)
10. [한계와 다음 단계](#10-한계와-다음-단계)
11. [팀원 소개](#11-팀원-소개)

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
<img width="10576" height="4768" alt="image (4)" src="https://github.com/user-attachments/assets/165230ab-b1ea-480e-8b45-6b43cbdea35d" />


### **파이프라인 INPUT**

| 제공처 | 수집 대상 | 수집 방식 | 주기 | 규모 |
| --- | --- | --- | --- | --- |
| NYC DOT / NYC Open Data | 도로별 속도 데이터 | Socrata API| 5분 | 실제 관측 지점 125개 |
| NYC DCP / NYC Open Data | 도로망(LION) | Socrata API | 분기 1회 | 세그먼트 218,373개 |
| NYC TLC Data | 택시 운행 기록 | 정적 파일 다운로드 | 월 1회 | 월 수천만 건 |
| NYC TLC Data | 택시존 | 정적 파일 다운로드 | 최초 1회 | 263개 zone |
| MTA·Port Authority / NY Open Data | 도로·혼잡 통행료 | 크롤러 | 정책 변경 시 | — |


### **파이프라인 OUTPUT**

| type | 계산 방식 | 산출값 | 규모 | 업데이트 주기 |
| :---: | --- | --- | --- | --- |
| 세그먼트별 통과시간 (type1) | 길이 ÷ 가중평균 속도 | 30분 버킷 통과시간(초) | 약 1,000만 행 | 30분 |
| 세그먼트별 길이 (type2) | LION 원본 그대로 | 정적 길이값(m) | 약 22만 행 | LION 갱신 시(분기 1회) |
| 세그먼트별 택시 승차수요 (type3) | 최근 N주 평균 → zone→segment 확산 | 요일×30분 슬롯 평균 승차수 | 약 7,000만 건 ~ 1억 건| 한 달 |
| 세그먼트별 통행료 (type4) | 혼잡통행료 + 도로통행료 합산 | 세그먼트당 통행료 | — (통행료 대상만, 희소) | 통행료·LION 원본 갱신 시(요금표는 매달 1일 변경 여부 확인) |




## 4. AWS 아키텍처

<img width="5368" height="1688" alt="image (5)" src="https://github.com/user-attachments/assets/a33e1932-243a-457e-b406-0b264f01e0ea" />

| 구성 | 서비스 | 역할 |
| --- | --- | --- |
| Data Lake | S3 | Bronze/Silver/Gold 원본·중간·최종 데이터 저장 |
| 이미지 저장소 | ECR | Airflow·Spark 실행용 컨테이너 이미지 저장 |
| 오케스트레이션 | EC2(Airflow) | 파이프라인 스케줄링 및 실행 관리 |
| 대용량 처리 | EMR(Spark) | Silver/Gold 단계 데이터 정제·집계 |
| 서빙 저장소 | RDS(Gold DB) | 최종 지표(type1~4) 저장, API 조회 대상 |
| 서빙 API | Lambda | RDS 조회 + 인메모리 캐시로 응답 생성 |
| API 엔드포인트 | API Gateway | 외부 요청을 Lambda로 라우팅 |
| 대시보드 | S3(정적 호스팅) | 프론트엔드 대시보드 배포 |

※ EC2·EMR·RDS는 같은 VPC 안에서 통신합니다.

## 5. Airflow DAG 설계

타입마다 원본 데이터의 갱신 패턴이 달라, DAG 스케줄도 타입별로 다르게 설계했습니다.

- **폴링형(Type1)** — 원본이 5분마다 갱신되지만 정확히 언제 새 데이터가 올라오는지 보장이 안 돼, DAG를 30분(`*/30 * * * *`)마다 돌려 새 데이터 유무를 확인합니다.
- **이벤트 기반(Type2, Type4)** — LION·통행료 원본은 갱신 빈도가 낮고 불규칙해서, 고정 스케줄 대신 Airflow Asset으로 "원본이 갱신되면" 트리거되도록 설계했습니다.
- **일 단위 확인형(Type3)** — TLC 원본이 월 단위로 불규칙하게 공개돼, 매일(`@daily`) 새 데이터 유무만 확인하고 실제로 있을 때만 재계산합니다.

**재실행/중복 방지** — Gold 적재는 `(segment_id, sk)` upsert라 재실행해도 중복이 쌓이지 않고, Type3는 재계산 조건(기간·매핑버전)을 워터마크로 판단해 불필요한 재계산을 막습니다.

**S3 Staging Lifecycle** — DAG 실행 중 실패로 남은 임시 스테이징 결과만 7일 뒤 자동 삭제합니다(`config/s3-staging-lifecycle.json`).

**실제 이슈 대응** — EC2 CPU 경합으로 Airflow DagBag import가 timeout(30초)을 넘겨 죽던 문제를 120초로 조정해 해결했고, 프로토타입 단계 DynamoDB 쓰기 32-way 병렬이 처리량 한도를 넘겨 죽던 문제를 10-way + adaptive 재시도로 해결했습니다.

## 6. 예외 처리 및 스키마 검증

**데이터 품질 검증(Great Expectations)** — Bronze 적재 시 taxi_type별 필수 컬럼이 다 있는지 검증합니다. Silver1 변환 로직과 완전히 같은 컬럼 매핑을 공유해, 검증 기준과 실제 변환 로직이 서로 어긋나는 걸 방지합니다. 검증 실패 시 해당 파일은 처리에서 제외됩니다.

**삭제된 세그먼트 정리** — LION 도로망이 갱신돼 사라진 세그먼트는, 최신 유효 세그먼트 집합에 없는 기존 행을 안티조인으로 찾아 자동 삭제합니다. 유효 집합이 비어 있으면 상류 버그로 보고 삭제 대신 예외를 던져, 테이블 전체가 실수로 비워지는 걸 막습니다.

**인프라 장애 시 대체 경로** — 내비게이션 경로 계산은 여러 세그먼트 값을 실시간으로 조합해야 해서, 값 하나가 지연되면 경로 계산 전체가 막힙니다. 그래서 RDS 자체가 응답 불가능하면(연결 실패, 타임아웃) Lambda 메모리 캐시 → S3 Gold 스냅샷(마지막 정상 데이터) → 코드 하드코딩 기본값 순으로 내려갑니다. 연결·쿼리 타임아웃을 1초로 짧게 걸고 재시도는 하지 않아, 장애 상황에서 응답이 지연되는 대신 곧바로 다음 단계로 넘어갑니다.

## 7. 모니터링

RDS 쿼리 응답시간(p50/p95/p99), 타입별 fallback 계층 비율, API 전체 응답시간을 Grafana로 봅니다. fallback 비율은 사용자가 실제로 얼마나 자주 부정확한 값을 받는지 보여줘 개선 우선순위를 정하는 근거가 되고, API·RDS·Lambda 응답시간을 나란히 보면 병목이 RDS인지 Lambda 콜드스타트인지 구분할 수 있습니다.

**모니터링으로 실제 장애를 잡은 사례** — Type3 배치(Gold 갱신) 파이프라인이 서빙 테이블에 대량 upsert를 하는 동안, 조회 쿼리가 그 쓰기 락에 걸려 statement_timeout(1초)으로 계속 취소되는 장애를 Grafana에서 발견했습니다. 하드코딩 폴백 비율이 평소 대비 93.5%까지 치솟은 걸 보고, RDS CPU(30%대)·디스크 지연(10~20ms)은 정상인데 DatabaseConnections만 57→76으로 튄 걸 확인해 원인이 디스크·CPU가 아니라 쓰기 트랜잭션의 락 대기라는 걸 알아냈습니다. 해결책은 행 단위 upsert 대신 새 데이터를 별도 테이블에 완전히 채운 뒤 테이블 이름만 원자적으로 스왑(RENAME)하는 방식으로 바꾼 것입니다. 인덱스도 데이터를 다 채운 뒤 한 번에 생성합니다 — 처음엔 인덱스가 있는 빈 테이블에 채워 넣다가 행마다 B-tree를 갱신하느라 20분 넘게 끝나지 않는 사고가 있었고, 정렬 후 한 번에 인덱스를 쌓는 방식(`ADD PRIMARY KEY`)으로 바꿔 해결했습니다.

## 8. 기술적 고민과 결정

각 결정의 배경(관측된 사실, 고려한 대안, 기각 사유, 검증 방법)은 링크한 문서에서 자세히 볼 수 있습니다.

| # | 고민 | 결정 | 링크 |
| :---: | --- | --- | --- |
| 1 | (예시) 서빙 저장소로 무엇을 쓸까 → | (예시) 비용 이슈로 DynamoDB 대신 RDS(PostgreSQL) 선택 | [상세](docs/decisions/01-example.md) |
| 2 |  |  | [상세](docs/decisions/02-decision.md) |
| 3 |  |  | [상세](docs/decisions/03-decision.md) |
| 4 |  |  | [상세](docs/decisions/04-decision.md) |
| 5 |  |  | [상세](docs/decisions/05-decision.md) |
| 6 |  |  | [상세](docs/decisions/06-decision.md) |
| 7 |  |  | [상세](docs/decisions/07-decision.md) |
| 8 |  |  | [상세](docs/decisions/08-decision.md) |
| 9 |  |  | [상세](docs/decisions/09-decision.md) |
| 10 |  |  | [상세](docs/decisions/10-decision.md) |
| 11 |  |  | [상세](docs/decisions/11-decision.md) |
| 12 |  |  | [상세](docs/decisions/12-decision.md) |
| 13 |  |  | [상세](docs/decisions/13-decision.md) |
| 14 |  |  | [상세](docs/decisions/14-decision.md) |

## 9. 기술 스택

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

> DynamoDB도 검토했지만 비용 이슈로 RDS(PostgreSQL)를 최종 채택 — 자세한 배경은 [8. 기술적 고민과 결정](#8-기술적-고민과-결정)을 참고하세요.

## 10. 한계와 다음 단계

- Type3는 zone 평균값을 그 zone에 속한 모든 세그먼트에 동일하게 적용합니다 — 세그먼트별 가중치(도로 유형, 위치 등)를 반영해 차등화하는 방안을 검토 중입니다.
- RDS 전환 후 실제 장애 상황의 응답 지연·성공률은 별도 측정이 필요합니다.
- Type3(택시 승차 수)은 실제 확률이 아닌 과거 평균 근사치 — 날씨·이벤트 변수로 확장 가능합니다.
- 신선도(freshness) 임계값이 현재 고정값 — 세그먼트별 동적 임계값으로 개선할 수 있습니다.
- Gold 파이프라인이 아직 끝나지 않은 시점에 요청이 들어오면 어떤 값을 반환해야 하는지는 아직 결론을 내리지 못했습니다.
- EMR 작업을 여러 개로 나눠 돌리는 대신 한 번에 묶어 돌리는 방안과, 데이터 유입 자체를 확인하는 전용 대시보드를 검토 중입니다.
- RDS 커넥션 수를 점진적으로 줄여가며 병목이 어디서 생기는지 실험적으로 확인하는 작업이 남아 있습니다.

## 11. 팀원 소개

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
