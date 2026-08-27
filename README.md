<h1 align="center">🌐 택시 내비게이션용 도로 정보 API</h1>

<p align="center">
내비게이션 경로 계산에 필요한 도로 세그먼트별 정보(소요시간·길이·통행료 등)를 데이터 파이프라인으로 구축해 API로 제공합니다.<br>
택시 전용 기능으로, 도로 세그먼트별 택시 승차 승객 수 정보도 함께 제공합니다.
</p>

<p align="center">
  <a href="https://nav-api-dashboard-lsy341.s3-website.ap-northeast-2.amazonaws.com"><img src="https://img.shields.io/badge/대시보드_바로가기-000000?style=for-the-badge&logoColor=white" alt="대시보드"/></a>
  <a href="http://3.38.96.76:8080"><img src="https://img.shields.io/badge/Airflow_바로가기-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/></a>
</p>

<p align="center">
<sub>소프티어 부트캠프 8기 · Data Engineering 5조 (three-idiots)</sub>
</p>

---

## 목차

1. [개요](#1-개요)
2. [주요 기능](#2-주요-기능)
3. [데이터 파이프라인](#3-데이터-파이프라인)
4. [시스템 아키텍처](#4-시스템-아키텍처)
5. [데이터 모델링과 서빙 저장소](#5-데이터-모델링과-서빙-저장소)
6. [운영과 성능](#6-운영과-성능)
7. [기술 스택](#7-기술-스택)
8. [한계와 향후 개선](#9-한계와-향후-개선)
9. [팀원 소개](#10-팀원-소개)


## 1. 개요

경로 탐색은 내비게이션의 역할이고, 저희는 그 알고리즘이 신뢰하고 쓸 수 있는 세그먼트별 값을 미리 계산·저장·서빙합니다. 분석 결과(4가지 지표 값)가 곧 최종 서비스(API 응답)이기 때문에, 기획 목적과 결과물이 그대로 연결됩니다.

> 🎯 최우선 원칙: 정확성보다 가용성이 먼저입니다. 파이프라인이나 데이터 일부에 장애가 있어도 API는 무조건 어떤 값이든 응답합니다 ([5.3 Fallback 설계](#53-type1-통행-소요시간-4단계-fallback) 참고).

접근 패턴이 항상 `segments 목록 + type 1개 + date/time` → 그 타입의 값들이라, 조인·집계는 파이프라인이 미리 끝내고 서빙은 순수 키 조회만 담당하도록 레이어를 분리했습니다(Bronze → Silver1 → Silver2 → Gold1/Gold2 → Serving). 타입마다 소스·계산 로직·갱신주기가 전부 달라 파이프라인과 서빙 테이블도 타입별로 분리해, 팀원별 독립 개발과 장애 격리를 얻습니다.


## 2. 주요 기능

**도로 정보 데이터 파이프라인 구축**

| type | 지표 | 경로 추천 용도 | 방향 |
| :---: | --- | --- | :---: |
| 1 | 도로 세그먼트별 통행 소요시간 | 빠른 경로 | 최소화 |
| 2 | 도로 세그먼트별 길이 | 짧은 경로 | 최소화 |
| 3 | 도로 세그먼트별 택시 승차 승객 수 (택시 전용) | 승객 많은 경로 | 최대화 |
| 4 | 도로 세그먼트별 통행료 | 무료 경로 | 최소화 |

> type=3은 `dropoff`가 아니라 `pickup` 기준입니다 — 최단 경로가 아니라 **택시기사가 승객을 만날 가능성이 높은 경로**를 위한 지표입니다. TLC 원본은 Taxi Zone 단위라 Zone↔도로 세그먼트 매핑을 거칩니다.
>
> type=4(통행료)는 혼잡통행료+도로통행료를 세그먼트당 합산 값으로 제공합니다. 경로 전체 합산 시 `sum`이 아니라 `max`로 집계해야 합니다(혼잡통행료 중복 청구 방지).

**API 제공**: 구축된 도로별 정보 데이터 조회 API 1종 ([8. 서비스 정보](#8-서비스-정보) 참고)


## 3. 데이터 파이프라인

| 계층 | 역할 |
| --- | --- |
| Bronze | 원본 그대로 수집 |
| Silver1 | 컬럼명·타입 통일, 결측/이상치 정리 |
| Silver2 | 소스 간 구조적 매핑/조인 (Zone↔Segment 등) |
| Gold1 → Gold2 | 유효성 필터링 → 최종 지표 계산 + 서빙 저장소 upsert |
| Serving | 계산 없이 저장값 조회 (Fallback만 수행) |

**소스 데이터 출처 및 갱신 주기**

| 소스 | 제공 정보 | 갱신 주기 | 수집 방식 |
| --- | --- | --- | --- |
| NYC TLC Data | 뉴욕시내 택시 운행 기록 | 월 1회 | 정적 파일 다운로드 |
| NYC TLC Data | 택시존 | 최초 1회 | 정적 파일 다운로드 |
| NYC Open Data | 도로망(LION) 데이터 | 분기 1회 | Socrata API |
| NYC Open Data | 도로별 속도 데이터 | 5분 | Socrata API |
| MTA Data | 도로통행료 부과 구간·금액 | 정책 변경 시 | 크롤러 |
| MTA Data | 혼잡통행료 부과 구간·금액 | 정책 변경 시 | 크롤러 |

품질 검증은 Great Expectations(TLC Bronze)로, 대규모 조인/집계는 PySpark on EMR Serverless로, 전체 흐름은 Airflow DAG가 오케스트레이션합니다. 설정값은 환경변수로 분리하고, 저장소 조회 모듈은 예외를 삼키지 않고 그대로 던져 호출부가 "값 없음"과 "호출 실패"를 구분해 Fallback으로 넘어가게 합니다.


## 4. 시스템 아키텍처

사진 넣자!!

<p align="center">
  <img src="🚧 TODO: 아키텍처 다이어그램 이미지 URL" width="100%" alt="시스템 아키텍처">
</p>



## 5. 데이터 모델링과 서빙 저장소

### 5.1 왜 RDS(PostgreSQL)인가 — DynamoDB 대비 비용 이슈

접근 패턴(key-value 조회)만 보면 DynamoDB(멀티 AZ 복제, failover 없음)가 먼저 떠올랐지만, 이 프로젝트 규모 대비 비용이 과했다고 판단해 최종적으로 **RDS PostgreSQL**을 서빙 저장소로 결정했습니다. 이 결정으로 낮아질 수 있는 가용성은, [5.3 Fallback 체인](#53-type1-통행-소요시간-4단계-fallback)으로 애플리케이션 레벨에서 보완합니다.

### 5.2 Gold 테이블 스키마 (타입별)

4가지 지표는 전부 `(segment_id, sk) 복합키 → value` 형태를 공유하지만, 타입마다 소스와 grain(시간/요일 차원 유무)이 달라 **테이블을 4개로 분리**했습니다 — 타입마다 다른 필드를 억지로 한 테이블에 우겨넣지 않아도 되고, DynamoDB → RDS 전환도 스키마 재설계가 아니라 저장소 클라이언트 어댑터 교체로 끝났습니다.

```sql
-- type1: 도로별 통과시간 — 버킷(실측)/AVG(과거평균)/SPEC(추정) 3종 행 공존
CREATE TABLE segment_metrics_type1 (
  segment_id     TEXT NOT NULL,
  sk             TEXT NOT NULL,     -- "0830"(버킷) | "AVG" | "SPEC"
  value          NUMERIC NOT NULL,  -- 통과시간(초)
  collected_date DATE,              -- 버킷 항목만: 원본 관측일
  observed_at    TIMESTAMPTZ,       -- 버킷 항목만: freshness 판정용
  count          INTEGER,           -- AVG 항목만: 증분 평균 갱신용
  PRIMARY KEY (segment_id, sk)
);

-- type2: 도로별 길이 — 세그먼트당 1행, 시간 무관
CREATE TABLE segment_metrics_type2 (
  segment_id TEXT NOT NULL,
  sk         TEXT NOT NULL DEFAULT 'LENGTH',
  value      NUMERIC NOT NULL,      -- 길이(m)
  PRIMARY KEY (segment_id, sk)
);

-- type3: 도로별 택시 승차 승객 수 — 값 행 + 재계산 판단용 메타 행 1개 공존
CREATE TABLE segment_metrics_type3 (
  segment_id      TEXT NOT NULL,    -- 값 행: 실제 segment_id / 메타 행: '__META__'
  sk              TEXT NOT NULL,    -- "3#MON#0900"(값) | "TYPE#3"(메타)
  value           NUMERIC,          -- 값 행만: 요일+30분 슬롯별 평균 승차 수
  status          TEXT,             -- 메타 행만: 재계산 완료 여부
  window_start    DATE,             -- 메타 행만
  window_end      DATE,             -- 메타 행만
  rolling_weeks   INTEGER,          -- 메타 행만
  mapping_version TEXT,             -- 메타 행만: zone-segment 매핑 버전
  updated_at      TIMESTAMPTZ,      -- 메타 행만
  PRIMARY KEY (segment_id, sk)
);

-- type4: 도로별 통행료 — 세그먼트당 1행, 혼잡+도로 통행료 합산값
CREATE TABLE segment_metrics_type4 (
  segment_id TEXT NOT NULL,
  sk         TEXT NOT NULL DEFAULT 'TYPE#4',
  value      NUMERIC NOT NULL,      -- 혼잡통행료 + 도로통행료
  PRIMARY KEY (segment_id, sk)
);
```

### 5.3 type=1 통행 소요시간 4단계 Fallback

원천 교통 데이터는 항상 완벽하게 들어온다고 보장할 수 없습니다(원천 API 장애·결측·이상값·오래된 값·신규 세그먼트). 데이터가 없다는 이유만으로 응답 자체를 못 하는 상황을 막기 위해, 신뢰도 순서대로 내려가는 4단계 체인을 둡니다 — 아래로 갈수록 정확도는 낮아지지만 응답 가능성은 높아집니다.

1. **최신 실측값(Fresh Exact)** — `observed_at`이 신선도 기준 이내일 때만 채택 (값 존재 여부뿐 아니라 최신인지까지 확인)
2. **세그먼트 과거 평균(Historical Average)** — 실시간 값이 오래됐으면 그 세그먼트의 과거 평균으로 대체
3. **도로 스펙 기반 추정(Spec Estimate)** — 실측 데이터 계통 자체가 무너졌다면 `길이 ÷ 제한속도`로 완전히 다른 데이터로 추정
4. **코드 기본값(Hardcoded Default)** — 스펙조차 없는 최악의 경우, 애플리케이션 상수 반환

키가 없는 경우와 저장소 호출 자체가 실패(타임아웃/에러)하는 경우 모두 **동일하게** 다음 단계로 내려갑니다. type2/3/4도 같은 원칙의 축약된 체인(`값 → 코드 상수`)을 갖습니다.


## 6. 운영과 성능

- **스케줄링/재실행** — Airflow DAG로 타입별 파이프라인을 독립 스케줄링, `@task.short_circuit`으로 새 데이터가 없으면 다운스트림을 건너뜁니다. Gold 적재는 `(segment_id, sk)` upsert라 재실행해도 중복이 쌓이지 않고, type3는 재계산 조건(기간·매핑버전)을 워터마크로 판단해 불필요한 재계산을 막습니다.
- **S3 Staging Lifecycle** — 파이프라인 실패로 남은 임시 결과만 7일 뒤 자동 삭제합니다(Bronze·운영 Silver/Gold는 대상 아님, `config/s3-staging-lifecycle.json`). `put-bucket-lifecycle-configuration`은 기존 설정 전체를 교체하므로, 적용 전 기존 버킷 규칙을 확인해 병합해야 합니다.
- **실제로 겪은 이슈** — EC2(vCPU 2개)에서 태스크가 몰리며 Airflow DagBag import가 30초 timeout을 넘겨 죽던 문제를 timeout 120초로 해결했고, 프로토타입 단계에서 DynamoDB 쓰기 32-way 병렬이 처리량 한도를 넘겨 죽던 문제를 10-way + adaptive 재시도로 해결했습니다.
- **캐싱과 빠른 실패** — 서빙 API는 최근 조회 값을 in-memory LRU 캐시(최대 5만 건)로 재사용하고, 저장소 클라이언트의 타임아웃을 1초로 짧게 둬서 느려지면 오래 기다리는 대신 빨리 실패해 Fallback으로 넘어가게 합니다.
- **O(1) 증분 집계** — type1 세그먼트 평균은 48개 버킷을 매번 다시 읽지 않고, 이번에 바뀐 버킷 하나만 반영하는 증분 공식으로 계산합니다.


## 7. 기술 스택

| 영역 | 스택 |
| --- | --- |
| **오케스트레이션** | ![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white) |
| **대용량 처리** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white) ![AWS EMR Serverless](https://img.shields.io/badge/EMR%20Serverless-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white) |
| **데이터 품질** | ![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6310?style=for-the-badge&logo=greatexpectations&logoColor=white) |
| **저장소** | ![S3](https://img.shields.io/badge/S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white) ![Amazon RDS](https://img.shields.io/badge/RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white) |
| **서빙 API** | ![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white) |
| **인프라** | ![EC2](https://img.shields.io/badge/EC2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white) |
| **CI/CD** | ![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white) |
| **언어/라이브러리** | ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) pandas · geopandas · shapely · boto3 |

> DynamoDB도 검토했지만 비용 이슈로 RDS(PostgreSQL)를 최종 채택했습니다 ([5. 데이터 모델링과 서빙 저장소](#5-데이터-모델링과-서빙-저장소) 참고).


## 8. 한계와 향후 개선

- RDS 전환 후 실제 장애 상황에서의 응답 지연/성공률은 별도 측정이 필요합니다.
- type=3(택시 승차 수) 데이터는 실제 확률이 아니라 과거 평균 근사치입니다 — 날씨·이벤트 등 변수로 확장할 수 있습니다.
- 신선도(freshness) 임계값이 현재 고정값이라, 세그먼트별 동적 임계값으로 개선할 수 있습니다.


## 9. 팀원 소개

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
