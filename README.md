<h1 align="center">🌐 택시 내비게이션용 도로 정보 API</h1>

<p align="center">
내비게이션 경로 계산에 필요한 도로 세그먼트별 정보(소요시간·길이·통행료 등)를 데이터 파이프라인으로 구축해 API로 제공합니다.<br>
택시 전용 기능으로, 도로 세그먼트별 택시 승차 승객 수 정보도 함께 제공합니다.
</p>

<p align="center">
  <a href="https://nav-api-dashboard-lsy341.s3-website.ap-northeast-2.amazonaws.com"><img src="https://img.shields.io/badge/대시보드_바로가기-000000?style=for-the-badge&logoColor=white" alt="대시보드"/></a>
  <a href="http://52.79.216.11:8080"><img src="https://img.shields.io/badge/Airflow_바로가기-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/></a>
  <a href="🚧 Grafana URL"><img src="https://img.shields.io/badge/Grafana_바로가기-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana"/></a>
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
8. [향후 계획](#8-향후-계획)
9. [팀원 소개](#9-팀원-소개)

## 1. 개요

경로 탐색은 내비게이션의 역할이고, 저희는 그 알고리즘이 신뢰할 수 있는 세그먼트별 값을 미리 계산·저장·서빙합니다.

> 🎯 최우선 원칙: 정확성보다 가용성. 데이터에 장애가 있어도 API는 무조건 값을 응답합니다 ([5.3 Fallback](#53-type1-통행-소요시간-4단계-fallback)).

요청이 항상 `segments + type 1개 + date/time`이라, 조인·집계는 파이프라인이 미리 끝내고 서빙은 키 조회만 담당합니다. 타입마다 소스·주기가 달라 파이프라인·테이블도 타입별로 분리했습니다.

## 2. 주요 기능

| type | 지표 | 경로 추천 용도 | 방향 |
| :---: | --- | --- | :---: |
| 1 | 도로 세그먼트별 통행 소요시간 | 빠른 경로 | 최소화 |
| 2 | 도로 세그먼트별 길이 | 짧은 경로 | 최소화 |
| 3 | 도로 세그먼트별 택시 승차 승객 수 (택시 전용) | 승객 많은 경로 | 최대화 |
| 4 | 도로 세그먼트별 통행료 | 무료 경로 | 최소화 |

> type=3은 `pickup` 기준(택시기사가 승객을 만날 가능성 지표), type=4는 혼잡+도로 통행료 합산값 — 경로 합산 시 `sum`이 아닌 `max`로 집계.

## 3. 데이터 파이프라인

### **INPUT**

| 제공처 | 수집 대상 | 수집 방식 · 주기 |
| --- | --- | --- |
| NYC DOT / NYC Open Data | 도로별 속도 데이터 | Socrata API · 5분 |
| NYC DCP / NYC Open Data | 도로망(LION), 세그먼트 약 10만 개 | Socrata API · 분기 1회 |
| NYC TLC Data | 택시 운행 기록 | 정적 파일 다운로드 · 월 1회 |
| NYC TLC Data | 택시존, 263개 zone | 정적 파일 다운로드 · 최초 1회 |
| MTA·Port Authority / NY Open Data | 도로·혼잡 통행료 | 크롤러 · 정책 변경 시 |

### **OUTPUT**

| type | 계산 방식 | 산출값 |
| :---: | --- | --- |
| 세그먼트별 통과시간 (type1) | 길이 ÷ 가중평균 속도 | 30분 버킷 통과시간(초) |
| 세그먼트별 길이 (type2) | LION 원본 그대로 | 정적 길이값(m) |
| 세그먼트별 택시 승차수요 (type3) | 최근 N주 rolling 평균 → zone→segment 확산 | 요일×30분 슬롯 평균 승차수 |
| 세그먼트별 통행료 (type4) | 혼잡통행료 + 도로통행료 합산 | 세그먼트당 통행료 |


## 4. 시스템 아키텍처

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

## 5. 데이터 모델링과 서빙 저장소

**5.1 왜 RDS(PostgreSQL)인가** — key-value 조회만 필요해 DynamoDB(멀티 AZ)가 먼저 떠올랐지만, 프로젝트 규모 대비 비용이 과해 RDS로 결정했습니다. 낮아진 가용성은 [5.3 Fallback](#53-type1-통행-소요시간-4단계-fallback)으로 보완합니다.

**5.2 Gold 테이블 스키마** — 4개 지표는 `(segment_id, sk) → value` 형태를 공유하지만 grain이 달라 테이블을 4개로 분리했습니다.

```sql
-- type1: 버킷(실측)/AVG(과거평균)/SPEC(추정) 3종 행 공존
CREATE TABLE segment_metrics_type1 (
  segment_id TEXT NOT NULL, sk TEXT NOT NULL,     -- "0830" | "AVG" | "SPEC"
  value NUMERIC NOT NULL, collected_date DATE,     -- 버킷만
  observed_at TIMESTAMPTZ, count INTEGER,          -- 버킷/AVG만
  PRIMARY KEY (segment_id, sk)
);

-- type2: 세그먼트당 1행
CREATE TABLE segment_metrics_type2 (
  segment_id TEXT NOT NULL, sk TEXT NOT NULL DEFAULT 'LENGTH',
  value NUMERIC NOT NULL, PRIMARY KEY (segment_id, sk)
);

-- type3: 값 행 + 재계산 판단용 메타 행 1개
CREATE TABLE segment_metrics_type3 (
  segment_id TEXT NOT NULL, sk TEXT NOT NULL,      -- "3#MON#0900" | "TYPE#3"(메타)
  value NUMERIC, status TEXT, window_start DATE, window_end DATE,
  rolling_weeks INTEGER, mapping_version TEXT, updated_at TIMESTAMPTZ,
  PRIMARY KEY (segment_id, sk)
);

-- type4: 세그먼트당 1행, 통행료 합산값
CREATE TABLE segment_metrics_type4 (
  segment_id TEXT NOT NULL, sk TEXT NOT NULL DEFAULT 'TYPE#4',
  value NUMERIC NOT NULL, PRIMARY KEY (segment_id, sk)
);
```

**5.3 type=1 통행 소요시간 4단계 Fallback** — 원천 데이터가 완벽하다고 보장할 수 없어, 신뢰도 순으로 내려가는 체인을 둡니다.

1. **최신 실측값** — `observed_at`이 신선도 기준 이내일 때만 채택
2. **세그먼트 과거 평균** — 실시간 값이 오래됐으면 대체
3. **도로 스펙 기반 추정** — `길이 ÷ 제한속도`로 다른 데이터 계통에서 추정
4. **코드 기본값** — 최후의 보루

키가 없는 경우와 저장소 호출 실패 모두 동일하게 다음 단계로 내려갑니다. type2/3/4도 축약된 체인(`값 → 코드 상수`)을 갖습니다.

## 6. 운영과 성능

- **재실행/중복 방지** — Gold 적재는 `(segment_id, sk)` upsert. type3는 워터마크(기간·매핑버전)로 재계산 필요 여부만 판단
- **S3 Staging Lifecycle** — 실패로 남은 임시 결과만 7일 뒤 자동 삭제(`config/s3-staging-lifecycle.json`). 기존 버킷 규칙과 병합 필요(`put-bucket-lifecycle-configuration`이 전체 교체이므로)
- **캐싱/빠른 실패** — 서빙 API는 LRU 캐시(5만 건) + 저장소 타임아웃 1초로, 느리면 바로 Fallback으로 넘김
- **실제 이슈 대응** — EC2 CPU 경합으로 Airflow DagBag import timeout 발생 → 120초로 조정. 프로토타입 단계 DynamoDB 32-way 쓰기 병렬이 처리량 한도 초과 → 10-way + adaptive 재시도로 해결

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
| **모니터링** | ![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white) |

> DynamoDB도 검토했지만 비용 이슈로 RDS(PostgreSQL)를 최종 채택 ([5. 데이터 모델링](#5-데이터-모델링과-서빙-저장소)).

## 8. 향후 계획

- RDS 전환 후 실제 장애 상황 응답 지연/성공률은 별도 측정 필요
- type=3(택시 승차 수)은 실제 확률이 아닌 과거 평균 근사치 — 날씨·이벤트 변수로 확장 가능
- 신선도(freshness) 임계값이 현재 고정값 — 세그먼트별 동적 임계값으로 개선 가능

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
