<h1 align="center">🌐 택시 내비게이션용 도로 정보 API</h1>

<p align="center">
내비게이션 경로 계산에 필요한 도로 세그먼트별 정보(소요시간·길이·통행료 등)를 데이터 파이프라인으로 구축해 API로 제공합니다.<br>
택시 전용 기능으로, 세그먼트별 택시 승차 승객 수 정보도 함께 제공합니다.
</p>

<p align="center">
  <a href="http://nav-api-dashboard-lsy341.s3-website.ap-northeast-2.amazonaws.com"><img src="https://img.shields.io/badge/대시보드_바로가기-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="대시보드"/></a>
  <a href="http://3.38.96.76:3000/d/nav-gold-overview/nav-gold-overview-rds-2b-emr-serverless?from=now-24h&to=now&timezone=browser&refresh=1m"><img src="https://img.shields.io/badge/Grafana_바로가기-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana"/></a>
</p>

<p align="center">
<sub>Grafana ID: admin</sub>
</p>
<p align="center">
<sub>Grafana PW: 1234</sub>
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
6. [스키마 검증](#6-스키마-검증)
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

타입마다 원본 데이터의 갱신 패턴이 달라, DAG 스케줄도 타입별로 다르게 설계했습니다. 9개 DAG는 트리거 방식 기준 **Cron 4개 + Asset 4개 + 수동 1개**로 나뉩니다.

```mermaid
graph LR
    zone["taxi_zone_pipeline<br/>(택시존 수집·정제)"]
    lion["lion_pipeline<br/>(LION 도로망 수집·정제)"]
    tollb["toll_bronze_pipeline<br/>(통행료 수집, 수동)"]
    tlcIngest["tlc_ingest_pipeline<br/>(TLC 원본 수집·정제)"]

    zs["zone_segment_pipeline<br/>(Zone-Segment 매핑 생성)"]
    t2["segment_length_pipeline<br/>(Type2 길이 계산)"]
    t4["toll_silver_gold_pipeline<br/>(Type4 통행료 계산)"]

    t3["tlc_type3_serving_pipeline<br/>(Type3 수요 계산)"]
    t1["segment_time_pipeline<br/>(Type1 소요시간 계산)"]

    zone -->|taxi_zone_silver1_updated| zs
    lion -->|lion_dim_segment_ready| zs
    lion -->|lion_dim_segment_ready| t2
    lion -->|lion_bronze_updated| t4
    tollb -->|toll_bronze_updated| t4
    zs -->|map_zone_segment_ready| t3
    tlcIngest -->|tlc_type3_gold2_ready| t3
    lion -.->|dim_segment 런타임 참조| t1
    t2 ~~~ t1
```

> `segment_time_pipeline`은 Asset 의존이 없어 30분마다 독립적으로 실행되지만, 실행 중 `lion_pipeline`이 만든 최신 `dim_segment.parquet`을 코드 레벨로 참조합니다(점선으로 표시, Asset 트리거는 아님).

**Cron 스케줄 (4개)**

| DAG | 주기 | 역할 |
| --- | --- | --- |
| lion_pipeline | 매일 04:00 (`0 4 * * *`) | LION 도로망 원본 수집·정제 |
| taxi_zone_pipeline | 매월 1일 04:00 (`0 4 1 * *`) | 택시존 원본 수집·정제 |
| segment_time_pipeline | 30분마다 (`*/30 * * * *`) | Type1(소요시간) 계산 |
| tlc_ingest_pipeline | 매주 월요일 04:00 (`0 4 * * 1`) | TLC 원본 수집·정제 |

**Asset 트리거 (4개)** — `|`는 OR 조건, 연결된 Asset 중 하나만 갱신돼도 실행됩니다.

| DAG | 의존 Asset | 발행 DAG | 설명 |
| --- | --- | --- | --- |
| segment_length_pipeline | `lion_dim_segment_ready` | lion_pipeline | LION 도로망이 갱신되면 Type2(길이)를 다시 계산한다 |
| zone_segment_pipeline | `lion_dim_segment_ready` \| `taxi_zone_silver1_updated` | lion_pipeline, taxi_zone_pipeline | LION 도로망 또는 택시존 정보가 바뀌면 Zone-Segment 매핑을 다시 만든다 |
| toll_silver_gold_pipeline | `toll_bronze_updated` \| `lion_bronze_updated` | toll_bronze_pipeline, lion_pipeline | 통행료 원본 또는 LION 도로망이 바뀌면 Type4(통행료)를 다시 계산한다 |
| tlc_type3_serving_pipeline | `tlc_type3_gold2_ready` \| `map_zone_segment_ready` | tlc_ingest_pipeline, zone_segment_pipeline | TLC 집계 결과 또는 Zone-Segment 매핑이 바뀌면 Type3(수요)를 다시 서빙한다 |

**수동 트리거 (1개)**

| DAG | 역할 |
| --- | --- |
| toll_bronze_pipeline | 통행료 원본 수집. 요금표 변경 여부를 사람이 확인한 뒤 직접 실행 |

### DAG별 태스크 목록

<details>
<summary>lion_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| ingest_lion | LION 원본 데이터 수집(Bronze) |
| build_dim_segment_staged | dim_segment 스테이징 테이블 빌드 |
| validate_staged_dim_segment | 스테이징 데이터 검증 |
| publish_dim_segment | 검증 통과한 dim_segment 게시(운영 반영) |
| cleanup_dim_segment_staging | 스테이징 임시 데이터 정리 |

</details>

<details>
<summary>taxi_zone_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| ingest_taxi_zone_shapefile | 택시존 shapefile 원본 수집 |
| build_taxi_zone_silver1 | 택시존 Silver1 정제(변경 없으면 게시 스킵) |

</details>

<details>
<summary>toll_bronze_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| upload_rates_task | 통행료 요금표 업로드 |
| upload_facilities_task | 통행료 부과 시설 목록 업로드 |
| upload_cbd_geofence_task | 혼잡통행료 구역(CBD) geofence 업로드 |
| publish_toll_bronze | 통행료 Bronze 게시 |

</details>

<details>
<summary>toll_rate_monitor</summary>

| 태스크 | 역할 |
| --- | --- |
| send_reminder | 매달 요금표 변경 여부 확인 Slack 알림 |

</details>

<details>
<summary>segment_time_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| check_new_data | 신규 속도 데이터 존재 확인(short-circuit) |
| collect_bronze | 도로 속도 데이터 수집(Bronze) |
| check_dim_segment_exists | dim_segment 존재 여부 확인(short-circuit) |
| submit_nav_time_job | Type1 Silver/Gold 계산 작업 제출(EMR) |

</details>

<details>
<summary>tlc_daily</summary>

| 태스크 | 역할 |
| --- | --- |
| generate_incremental_download_list | 신규 다운로드 대상 목록 생성 |
| download_file | TLC 파일 다운로드(taxi_type별 병렬) |
| validate_download | 다운로드 결과 검증 |
| store_bronze | Bronze 저장 |
| find_pending_silver_files | Bronze는 있지만 Silver1 미완료인 파일 복구 |
| chunk_bronze_files | taxi_type별 청크 묶기 |
| validate_bronze_quality | 청크별 Bronze 품질 검증(Great Expectations) |
| build_silver | 검증 통과 파일 Silver1 변환·저장 |
| find_pending_type3_months | Type3 처리 대상 월 탐색 |
| build_type3_staged_records | Type3 임시(스테이징) 레코드 생성 |
| validate_type3_staged_records | Type3 스테이징 레코드 검증 |
| publish_type3_daily_records | 검증 통과분 운영 파티션으로 승격 |
| cleanup_type3_staging | Type3 스테이징 정리 |
| check_type3_publish_needed | RDS 값이 최신 N주보다 오래됐는지 판단 |
| check_type3_reference_ready | zone-segment 매핑 준비 여부 확인 |
| publish_type3_rolling_values | Type3 롤링 평균 RDS 게시 |

</details>

<details>
<summary>segment_length_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| build_gold2_lion | Gold 계산용 LION 데이터 빌드 |
| submit_nav_length_job | Type2 계산 작업 제출(EMR) |
| build_and_write_spec_estimates | 스펙 기반 추정치 계산·기록 |

</details>

<details>
<summary>zone_segment_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| validate_reference_inputs | 참조 입력(LION·택시존) 유효성 검증 |
| build_map_zone_segment_staged | zone-segment 매핑 스테이징 빌드 |
| validate_staged_map_zone_segment | 매핑 스테이징 검증 |
| publish_map_zone_segment | 검증 통과한 매핑 게시 |

</details>

<details>
<summary>toll_silver_gold_pipeline</summary>

| 태스크 | 역할 |
| --- | --- |
| find_latest_lion_gdb | 최신 LION GDB 파일 탐색 |
| build_lion_facility_mapping_task | 시설-세그먼트 매핑 빌드 |
| build_lion_cbd_mapping_task | CBD(혼잡구역)-세그먼트 매핑 빌드 |
| build_and_write_gold | Type4 Gold 계산·RDS 기록 |

</details>

## 6. 스키마 검증

| 검증 지점 | 검증 방식·라이브러리 | 코드에 정의한 스키마 | 검증 예시 | 구현 |
| --- | --- | --- | --- | --- |
| **API 요청·응답** | FastAPI + Pydantic의 `BaseModel`, `Field`, `Literal` | `segment_ids` 1-500개, type은 1-4, 날짜는 `YYYY-MM-DD`, 시간은 `HH:MM`, 응답은 숫자 배열로 제한 | `type=5`, 빈 경로, `25:00` 요청은 FastAPI가 422로 거부 | [`src/serving/nav_api.py`](src/serving/nav_api.py) |
| **TLC Bronze 원본** | Great Expectations의 `ExpectColumnToExist`를 PySpark DataFrame에 실행 | 택시 종류마다 서로 다른 필수 원본 컬럼을 검사 | Yellow Taxi 파일에 `tpep_dropoff_datetime`이 없으면 critical 스키마 실패로 판정 | [`src/tlc/expectations.py`](src/tlc/expectations.py), [`src/tlc/bronze_validation.py`](src/tlc/bronze_validation.py) |
| **TLC Silver1 공통 스키마** | PySpark SQL의 `StructType`, `StructField`, `cast` | 네 종류의 TLC 데이터를 `timestamp 2개 + integer 3개 + double 1개`의 공통 6개 컬럼으로 변환 | FHV에 원래 없는 `passenger_count`, `trip_distance`는 지정 타입의 nullable 컬럼으로 추가하고, 필수 원본 컬럼 누락은 거부 | [`src/tlc/silver1_transform.py`](src/tlc/silver1_transform.py) |
| **Zone-Segment 매핑** | pandas DataFrame의 `is_unique`, `isna`, `between`, `isin` | 모든 LION `segment_id`가 정확히 하나의 `zone_id`를 가져야 하며, zone은 1~263, 매핑 방식은 `contains` 또는 `nearest`만 허용 | 입력 세그먼트가 218,373개인데 매핑이 218,372개이거나 `segment_id`가 중복되면 검증 실패 | [`src/silver2/zone_segment.py`](src/silver2/zone_segment.py) |
| **Type3 시공간 스키마** | PySpark SQL DataFrame의 `filter`, `groupBy`, `distinct`, `count` | Zone 결과는 `zone_id, type, date, time, value`, Segment 결과는 `segment_id, type, dow, time, value`로 고정하고 복합키와 전체 시간대 coverage를 검사 | 컬럼은 정상이더라도 특정 Zone의 14:30 값이 빠지면 `Zone × 날짜 × 48개 시간대` 예상 행 수와 달라 게시 중단 | [`src/tlc/gold2.py`](src/tlc/gold2.py) |
| **LION 원본 변경 전파** | Zone-Segment 매핑 결과를 `segment_id` 정렬 후 SHA-256 해시(`hashlib`) + Airflow Asset 트리거 | 매핑 내용의 해시를 `mapping_version`으로 저장해 "내용이 실제로 바뀌었는지"(run_id만으로는 구분 불가)를 판별 | LION이 갱신되면 Type2/4는 Asset(`lion_dim_segment_ready`/`lion_bronze_updated`)로 바로 재계산되고, Type3는 TLC 날짜 범위가 그대로여도 `mapping_version`이 달라지면 재계산이 트리거됨. Type1은 별도 트리거 없이 30분마다 최신 `dim_segment.parquet`을 직접 읽어 반영 | [`src/silver2/zone_segment.py`](src/silver2/zone_segment.py), [`src/tlc/type3_pipeline.py`](src/tlc/type3_pipeline.py) |

**LION 원본이 바뀌면 하위 데이터에 이렇게 전파됩니다**

LION은 거의 모든 지표가 기준으로 삼는 데이터라, `lion_pipeline`이 새 `dim_segment.parquet`을 발행하면(`lion_dim_segment_ready`/`lion_bronze_updated` Asset) 그 영향이 여러 타입으로 퍼집니다.

- **Type2(길이)** — `segment_length_pipeline`이 `lion_dim_segment_ready`에 바로 반응해 재계산합니다.
- **Type4(통행료)** — `toll_silver_gold_pipeline`이 `lion_bronze_updated`에 반응해 통행료 매핑을 다시 계산합니다.
- **Zone-Segment 매핑 → Type3(수요)** — `zone_segment_pipeline`이 매핑을 다시 만들면서, 그 결과를 `segment_id` 기준으로 정렬해 SHA-256으로 해시한 값을 `mapping_version`으로 저장합니다. `tlc_type3_serving_pipeline`은 TLC 원본 날짜 범위가 그대로여도 이 `mapping_version`이 이전과 다르면 재계산을 트리거합니다 — 매핑 run_id(uuid)만으로는 "내용이 실제로 바뀌었는지"를 구분할 수 없어서(재승격했지만 무관한 속성만 바뀐 경우도 있음), 내용 자체를 해시해 불필요한 재계산을 피합니다.
- **Type1(소요시간)** — 별도 트리거 없이, `segment_time_pipeline`이 30분마다 실행될 때마다 그 시점의 `dim_segment.parquet`을 코드 레벨로 직접 읽어 최신 상태를 자연히 반영합니다([5. Airflow DAG 설계](#5-airflow-dag-설계) 참고).

구현: [`src/silver2/zone_segment.py`](src/silver2/zone_segment.py), [`src/tlc/type3_pipeline.py`](src/tlc/type3_pipeline.py)

## 7. 모니터링

### 1. Grafana

[Grafana 대시보드 열기](http://3.38.96.76:3000/d/nav-gold-overview/nav-gold-overview-rds-2b-emr-serverless?from=now-24h&to=now&timezone=browser&refresh=1m)

| 관측 영역 | 주요 지표 | 확인 목적 |
| --- | --- | --- |
| 인프라 | RDS 및 EC2 CPU·메모리·디스크 점유율 | 병목이 DB 자원인지 애플리케이션인지 구분 |
| 서빙 | Lambda 호출·소요시간, API Gateway Latency | API 장애·지연 인지 |
| 파이프라인·데이터 | Airflow DAG 상태·태스크 소요시간·최근 실패, 타입별 row 수·마지막 갱신 시각 | DAG 성공 여부와 실제 RDS 데이터 신선도를 교차 검증 |
| 사용자 체감 성능 | RDS 쿼리 p50/p95/p99, 타입별 fallback 계층 비율 | 느린 쿼리와 부정확한 대체값 응답 비율을 함께 추적 |

<p align="center">
  <img width="100%" alt="Grafana RDS 및 EC2 자원 종합 현황" src="https://github.com/user-attachments/assets/04c1d5d7-7518-447e-8e1b-87563bf61eb1" /><br>
  <sub>RDS와 Airflow·Grafana가 동작하는 EC2의 CPU, 메모리, 디스크 상태</sub>
</p>

#### 모니터링으로 해결한 문제: RDS 쓰기 중 읽기 지연

Type3 Gold 데이터를 RDS 서빙 테이블에 대량 upsert하는 동안 같은 테이블을 읽는 API 쿼리의 응답시간이 증가했고, 일부 쿼리는 1초 `statement_timeout`으로 취소됐습니다. Grafana에서 다음 지표를 함께 비교해 원인을 좁혔습니다.

1. Type3 쿼리 p95/p99(p99는 약 1.7초까지 증가)
2. RDS 직접 조회 실패. 하드코딩 fallback 비율 최대 93.5%까지 급등
3. 같은 시각 RDS CPU와 디스크 지연은 정상 범위. `DatabaseConnections`는 57개에서 76개로 증가
4. 대량 upsert 트랜잭션과 API 읽기 쿼리 사이의 경합이 원인이라고 판단함.

<p align="center">
  <img width="70%" alt="Grafana Type3 RDS 쿼리 응답시간 p50 p95 p99" src="https://github.com/user-attachments/assets/09eba1fd-8e49-4cde-8bc6-7f6951e33328" /><br>
  <sub>Type3 쿼리 응답시간 분포: 평균만으로 숨겨지는 느린 요청을 p95·p99로 확인</sub>
</p>

<p align="center">
  <img width="100%" alt="Grafana Type1부터 Type4까지 fallback 계층 비율" src="https://github.com/user-attachments/assets/bcfab373-ef94-4828-b58a-1bd67b11ff24" /><br>
  <sub>타입별 RDS 직접 조회와 snapshot·hardcoded fallback 비율</sub>
</p>

해결책으로 운영 테이블에 행 단위 upsert를 반복하는 대신, 새 데이터를 별도 staging 테이블에 모두 적재하고 테이블 이름만 RENAME 하도록 변경했습니다.
API는 완성된 테이블만 읽으므로 배치 쓰기와 서빙 읽기가 같은 테이블에서 오래 경합하지 않습니다.

### 2. Airflow 장애 알림 (Slack)

| 구분 | 핵심 동작 |
| --- | --- |
| 태스크 실패 | 재시도 소진 후 DAG·Task·예외 요약과 Airflow 로그 링크를 Slack으로 전송 |
| 파일 검증 실패 | 제외된 TLC 파일과 사유를 청크당 한 메시지로 요약 |

구현: [`src/common/alerts.py`](src/common/alerts.py)

### 3. 로그

| 구분 | 핵심 동작 |
| --- | --- |
| 지표 로그 | RDS 쿼리 시간·fallback 계층 로그를 CloudWatch Logs Insights로 집계해 Grafana에 표시 |
| 장애 로그 | Lambda는 표준 출력으로 기록하고, EMR 실패 시 Spark Driver 로그 끝부분을 Airflow에 첨부 |

구현: [`src/common/logger.py`](src/common/logger.py), [`src/common/emr_serverless.py`](src/common/emr_serverless.py)


## 8. 기술적 고민과 결정

각 결정의 배경(관측된 사실, 고려한 대안, 기각 사유, 검증 방법)은 링크한 문서에서 자세히 볼 수 있습니다.

| # | 고민 | 결정 | 링크 |
| :---: | --- | --- | --- |
| 1 | zone→segment 확장 시 파티션이 몰리는 문제를 어떻게 풀까 | 파티션 기준을 zone_id에서 segment_id로 교체 (편차 105배 → 1.2배) | [상세](docs/decisions/01-skew.md) |
| 2 | Bronze 검증 실패를 어떻게 나눠서 대응할까 | 컬럼 존재 여부는 critical, 값 이상은 log-only로 구분 | [상세](docs/decisions/02-gx.md) |
| 3 | 여러 DAG가 EMR 자원을 나눠 쓰는 방법 | DAG별로 쓸 수 있는 자원 몫을 고정 배분 | [상세](docs/decisions/03-spark-tuning.md) |
| 4 | Type3 RDS 갱신을 어떤 방식으로 반영할까 | 파티션마다 임시 테이블에 나눠 담고, 다 담은 뒤 PK 생성 후 통째로 교체 | [상세](docs/decisions/04-rds-insert.md) |
| 5 | RDS 장애가 API 전체 실패로 이어지는 것을 어떻게 막을까? | 1초 타임아웃 후 메모리 캐시 → S3 스냅샷 → 기본값으로 단계적 폴백 | [상세](docs/decisions/05-decision.md) |
| 6 | 원천의 새 버전을 언제 처리 완료로 기록할까? | 변경 감지 마커는 운영 데이터 publish가 성공한 후에만 갱신 | [상세](docs/decisions/06-decision.md) |
| 7 | 검증된 데이터의 준비 완료를 downstream에 어떻게 전달할까? | DAG 직접 호출 대신 publish 완료 Asset을 발행하고 downstream에서 구독 | [상세](docs/decisions/07-decision.md) |
| 8 | 실측값이 결측일 때 코드 상수 대신 쓸 대체값을 어떻게 만들까 | (segment_id, time) 슬롯별 avg를 증분 공식으로 갱신해 같은 행에 저장 | [상세](docs/decisions/08-type1-avg.md) |
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


## 10. 한계와 다음 단계

| # | 한계 | 시도 | 다음 단계 |
| --- | --- | --- | --- |
| 1 | zone 안 모든 세그먼트에 동일한 수요값 적용 | zone×요일×시간대 평균을 세그먼트에 동일 적용 | 2016년 정밀좌표로 밀집 스팟 검증 후 세그먼트별 가중치 차등 적용 |
| 2 | TLC 데이터는 분석용 — 실시간 기사 판단 지표로는 부적합할 수 있음 | 과거 평균 근사치를 수요 지표로 그대로 서빙 | 실측 기반 유의미성 검증 |
| 3 | 속도 데이터 결측·수집 주기 불안정 (외부 API 의존 한계) | 도로 스펙(길이÷제한속도) 추정치로 백필 | 유사 도로 실측 통계 반영해 추정 정확도 개선 |
| 4 | 세그먼트 순차 누적 조회라 경로 길어질수록 오차 누적 | 보정 없이 누적 시각 그대로 순차 조회 | 재보정 체크포인트 또는 오차 범위 함께 반환 |
| 5 | 비용 고려 부족 — DynamoDB 과금, EMR 리소스 과다 사용 | 접근 패턴 검증 없이 DynamoDB 채택 → RDS로 전환 | 배치/즉시 작업 구분, 사전 비용 산정, Budget Alert 도입 |

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
