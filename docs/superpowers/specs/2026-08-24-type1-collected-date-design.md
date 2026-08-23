# Type1 버킷에 collected_date 추가 — 설계 문서

- 날짜: 2026-08-24
- 상태: 브레인스토밍 완료, 사용자 승인
- 관련 문서: [2026-08-21-segment-metrics-api-design.md](2026-08-21-segment-metrics-api-design.md)

## 1. 배경 / 목표

`SegmentMetricsType1` 테이블의 버킷 항목(`segment_id`, `sk`, `value`)은 값이 갱신될 때마다
같은 `(segment_id, sk)` 자리에 덮어써진다. 그래서 지금 저장된 값이 실제로 며칠자 원본
데이터로 계산된 것인지 알 방법이 없다. 이 정보를 항목에 같이 저장해서, 운영자가 필요할 때
DynamoDB를 직접 조회해 데이터 신선도를 확인할 수 있게 한다.

## 2. 범위

- **포함**: 버킷 항목(`sk`가 `"HHMM"` 버킷 키인 항목)에 `collected_date` 속성 추가
- **제외**: AVG 항목(`sk="AVG"`), GLOBAL/DEFAULT 항목 — 특정 하루로 대표할 수 없는 값이라 대상 아님
- **제외**: API/서빙 레이어 노출 — 내부(운영/디버깅)용으로만 쓴다. `POST /segments/values` 응답
  스키마는 변경하지 않는다
- **제외**: 테이블 스키마·키 구조 변경, 기존 항목 백필 — DynamoDB는 스키마리스라 새 속성은
  마이그레이션 없이 새 쓰기부터 바로 붙는다. 기존 97,216개 항목은 `collected_date` 없이 남아도
  무방하다(아직 아무도 이 필드를 읽지 않음)

## 3. `collected_date`의 의미와 산출 방법

그 버킷을 구성한 원본 속도 판독값들의 `observed_at`(`src/speed/silver1.py`가 Bronze의
`data_as_of`로부터 만든 컬럼, Silver2를 거쳐 Gold2까지 그대로 유지됨) 중 **최신 값의 날짜
부분**이다. 파이프라인이 실제로 이 값을 계산/기록한 시각(wall-clock)이 아니라, **원본 데이터가
관측된 날짜**를 의미한다 — 재처리(backfill)로 파이프라인이 며칠 뒤에 다시 돌아도
`collected_date`는 원본 데이터의 날짜를 그대로 반영해야 하기 때문이다.

`observed_at`은 `nav_time/gold2.py`의 `compute_time_seconds`가 버킷 그룹핑(`groupBy("segment_id",
"bucket")`)에 이미 쓰고 있는 컬럼이므로, 새로운 데이터 흐름(파일 경로·CLI 인자 전달)을
추가하지 않고 기존 집계에 한 단계만 더해서 얻는다.

## 4. 스키마 변경

**변경 전**
```
{'segment_id': '0342993', 'sk': '0600', 'value': 10}
```

**변경 후**
```
{'segment_id': '0342993', 'sk': '0600', 'value': 10, 'collected_date': '2026-08-24'}
{'segment_id': '0342993', 'sk': 'AVG', 'value': 9, 'count': 40}   # 변경 없음
```

## 5. 구현 지점

`src/nav_time/gold2.py` 한 파일만 수정한다.

1. `compute_time_seconds`: `groupBy("segment_id", "bucket")` 집계에
   `to_date(spark_max("observed_at")).alias("collected_date")`를 추가해서 반환 DataFrame에
   `collected_date` 컬럼을 싣는다.
2. `to_dynamodb_items`: 버킷 항목(`bucket_items`)을 만들 때 `"collected_date": row["collected_date"]`를
   같이 넣는다. `avg_items`(AVG 증분 갱신 로직)는 변경하지 않는다.

`src/common/dynamodb.py`, `src/common/config.py`, API/서빙 레이어(`src/serving/*`)는 변경하지 않는다.

## 6. 테스트 전략

`tests/nav_time/test_gold2.py`에 추가:
- `compute_time_seconds`가 반환하는 DataFrame에 `collected_date` 컬럼이 존재하고, 한 버킷 안에
  서로 다른 날짜의 판독값이 섞여 있으면(자정 경계 등 예외 케이스) 최신 `observed_at`의 날짜를
  택하는지 검증
- `to_dynamodb_items`가 만든 버킷 항목엔 `collected_date`가 있고, AVG 항목엔 없는지 검증

## 7. 향후 확장 (이번 범위 아님)

- API 응답에 `collected_date` 노출 (필요해지면 별도 설계)
- Type2/3/4에 동일 패턴 적용 여부 검토
