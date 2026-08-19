# Zone 내부 세그먼트 공간 가중치(spatial_weight) 설계

## 배경

`tlc_volume` 컴포넌트(`src/tlc/gold.py`)는 zone x hour 하차수를 그 zone에 속한 모든
세그먼트에 **동일하게** 복사한다(`_expand_zone_to_segment_hour`, 세그먼트 수로 나누지
않고 zone 총합을 그대로 복사). 이 설계는
`docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md`에 이미 문서화된
의도된 선택이었다 — 당시엔 zone x hour 집계만 있어서 zone 내부 분포를 알 방법이
없었다.

실제로는 하나의 zone(TLC Taxi Zone) 안에도 도로 구간(세그먼트)마다 실제 트립 밀집도가
크게 다르다(예: 큰 교차로/터미널 근처 vs 이면도로). 이번 설계는 **zone 내부에서 세그먼트별
상대적 밀집도(spatial_weight)를 산정**해서, 균등 분배 대신 이 가중치로 zone 총합을
나눠 갖도록 바꾼다.

이 산정에 쓸 재료는 2016년 TLC 하차 위경도를 0.0001도(≈8~11m) 그리드로 비닝한
BigQuery 결과(`temp/bq-results.csv`, 741,008행, 그리드 셀당 dropoff_count)다. TLC는
2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 이게 **위경도 기준으로 zone 내부
분포를 직접 볼 수 있는 마지막 해**다.

## 목표

세그먼트별로 "자기가 속한 zone 안에서 상대적으로 얼마나 밀집된 위치인가"를 나타내는
정적 참조 테이블(`map_segment_spatial_weight.parquet`)을 만들고, `tlc/gold.py`의
zone→segment 균등 분배 로직을 이 가중치 기반 분배로 교체한다.

이 값은 **2016년 한 해 스냅샷에서 한 번 계산하는 정적 값**이다. 주기적으로 재계산할
근거 데이터가 없으므로(TLC가 위경도 제공을 끊었음), DAG 연결이나 재실행 스케줄은
만들지 않는다.

**주의**: 이건 `map_zone_segment.parquet`과 다른 성격이다 — `map_zone_segment.parquet`은
LION이 분기마다 갱신될 때 `dags/lion_pipeline.py`가 **자동으로 재생성**한다. 반면
`map_segment_spatial_weight.parquet`은 그 자동 재생성 대상이 아니라서, LION 갱신으로
새 `segment_id`가 생기면 이 정적 테이블에는 없는 상태가 된다. `_expand_zone_to_segment_hour`
(`src/tlc/gold.py`)는 그런 결측 세그먼트를 `spatial_weight=1.0`으로 폴백 처리하지만,
`build_dim_segment_tlc_volume`은 결측 비율이 임계값(5%, `MAX_MISSING_SPATIAL_WEIGHT_FRACTION`)을
넘으면 하드 실패하도록 방어한다 — LION 분기 갱신 후에는 `src/mapping/segment_spatial_weight.py`의
빌드 파이프라인을 사람이 직접 재실행해야 한다.

## 범위

**포함**

- `temp/bq-results.csv`를 `bronze/tlc/hotspot_2016/dropoff_grid.parquet`로 영구 보관
  (재현성 확보 — `data/`가 `.gitignore` 대상이라 결과물이 날아가면 원본이 있어야
  다시 만들 수 있다. `temp/`는 `DATA_DIR` 바깥의 스크래치 위치라 이후 `DATA_DIR`를
  S3 등으로 옮길 때 같이 따라가지 않으므로, `bronze/`로 옮겨야 마이그레이션 경로에
  포함된다)
- `map_segment_spatial_weight.parquet` 생성 로직 + 검증 함수
- `src/tlc/gold.py::_expand_zone_to_segment_hour`를 이 가중치를 쓰도록 수정

**제외**

- Airflow DAG 연결 — 재실행할 근거 데이터가 없어 스케줄링이 무의미하다.
- Bronze/Silver 별도 모듈 분리, collect/build 태스크 분리 — `tlc/gold.py`가 그 구조를
  쓴 이유(3년치 Spark 스캔이 무거워서)가 여기엔 없다. 파일 하나(`build_`/`validate_`
  함수 형태)로 충분하다.
- 시간대(hour)별로 다른 공간 분포 반영 — `bq-results.csv`에 hour 정보가 없다. 모든
  시간대에 동일한 spatial_weight를 적용한다.
- `traffic_score_weights.yaml`/`scoring/traffic_score.py` 통합 — 기존
  `tlc_volume` 컴포넌트 내부 계산 방식만 바꾸는 것이라, 이미 있는 통합 지점은 그대로
  둔다.

## 데이터 흐름

```
temp/bq-results.csv (lat_bin, lon_bin, dropoff_count — 741,008행, EPSG:4326)
        │  그대로 복사(변환 없음)
        ▼
bronze/tlc/hotspot_2016/dropoff_grid.parquet

        │  EPSG:4326 -> EPSG:2263 좌표 변환 (LION/zone과 동일 좌표계로)
        ▼
grid point (Point geometry, EPSG:2263)
        │  Taxi Zone 폴리곤과 point-in-polygon (map_zone_segment.py의 STRtree 패턴)
        ▼
grid point + zone_id  (비매칭 포인트만 제외, 로그만 남김 — 이 단계는 도시 전체
                        zone 대상이라 borough로 걸러내지 않음. Manhattan 한정
                        필터링은 더 뒤 Gold 단계(build_dim_segment_tlc_volume)에서 함)
        │  zone_id별로 그룹화 -> 그 zone에 속한 세그먼트(map_zone_segment.parquet) 중
        │  point 반경 100ft 이내 전부를 후보로 삼아 거리 역가중(1/(distance+ε))으로
        │  dropoff_count를 나눠 배분 (반경 안에 하나도 없으면 zone 내 최근접 1개로
        │  fallback). zone 경계 밖 세그먼트로는 매칭 안 함
        ▼
grid point + segment_id + 배분된 dropoff_count(분수)
        │  segment_id별로 배분된 dropoff_count 합산 -> segment_hotspot_count
        │  (매칭 0건인 세그먼트도 map_zone_segment 기준으로 0으로 포함)
        ▼
segment_hotspot_count (zone 내 세그먼트 전부, 매칭 없으면 0)
        │  라플라스 스무딩 + zone 내부 정규화 (zone 합 = 1)
        ▼
map_segment_spatial_weight.parquet
```

zone 경계를 넘는 최근접 매칭을 막는 이유: 세그먼트가 자기가 속한 zone이 아닌 다른
zone의 grid point에 카운트를 받으면, 그 zone의 `spatial_weight` 합이 1을 넘거나
모자라게 되어 `_expand_zone_to_segment_hour`에서 zone 총합이 깨진다.

## 스키마

`data/silver/map_segment_spatial_weight.parquet`

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `segment_id` | string | LION 세그먼트 ID (`map_zone_segment`의 routable 세그먼트 전부) |
| `zone_id` | int | 소속 TLC zone |
| `segment_hotspot_count` | double | 2016년 그리드 기준 이 세그먼트에 배분된 dropoff_count 합 (거리 역가중 분배라 소수, 매칭 없으면 0) |
| `spatial_weight` | double (0,1] | zone 내부 정규화된 상대 가중치. **zone별로 합 = 1** |

행 수 = `map_zone_segment.parquet` 행 수와 동일(세그먼트마다 정확히 1행).

## 계산 로직

1. **Bronze 적재**: `temp/bq-results.csv`를 읽어 `_ingested_at`, `_source` 메타컬럼을
   붙여 `bronze/tlc/hotspot_2016/dropoff_grid.parquet`로 저장한다
   (`src/taxi_zone/bronze.py`와 동일 관례).
2. **좌표 변환**: `lat_bin`/`lon_bin`(EPSG:4326)을 Point로 만들고 `LION_CRS`
   (EPSG:2263)로 변환한다 (`src/common/config.py`의 `TICKETMASTER_CRS`/`LION_CRS`
   패턴과 동일하게 `BQ_HOTSPOT_CRS = "EPSG:4326"` 상수를 추가한다).
3. **zone 매칭**: Taxi Zone 폴리곤에 대해 point-in-polygon (STRtree)으로 zone_id를
   찾는다. 이 단계(`_match_points_to_zone`)는 borough로 걸러내지 않고 도시 전체
   zone을 대상으로 매칭한다 — 매칭 안 되는 포인트만 제외하고 건수를 로그로 남긴다
   (`map_zone_segment.py`의 미매칭 처리와 동일 패턴). Manhattan 한정 필터링은
   기존 관례(`src/tlc/gold.py`)와 동일하게 더 뒤 Gold 단계
   (`build_dim_segment_tlc_volume`)에서 이뤄진다.
4. **세그먼트 매칭 (zone 내부로 한정, 반경 + 거리 역가중)**: zone_id로 그룹화한 뒤,
   `map_zone_segment.parquet`에서 같은 zone_id에 속한 세그먼트 geometry만 후보로
   삼는다. 최근접 세그먼트 하나에만 몰아주면(winner-take-all) 교차로처럼 여러
   세그먼트가 인접한 곳에서 실제 분산을 왜곡하므로, `src/mapping/ticketmaster_lion.py`의
   buffer+nearest-fallback 패턴을 그대로 가져온다:
   - grid point 반경 `HOTSPOT_SEGMENT_BUFFER_FT`(100ft) 이내에 있는 세그먼트 전부를
     후보로 삼는다 (grid 셀 자체가 8~11m이라 venue-도로 매핑에 쓴 200ft보다 좁게 잡음).
   - 후보가 있으면, 각 후보 세그먼트까지의 거리 `d`로 `1/(d + ε)` 가중치를 매겨(가까울
     수록 더 많이 받도록) 정규화한 뒤 `dropoff_count`를 그 비율대로 나눠 배분한다.
     `ε`(`HOTSPOT_INVERSE_DISTANCE_EPSILON_FT`, 기본 1.0ft)는 point가 세그먼트 위에
     정확히 있어 거리가 0이 되는 경우의 0-division만 막는 역할이다.
   - 반경 안에 세그먼트가 하나도 없으면(도로가 드문 구역), zone 내 최근접 세그먼트
     1개로 fallback한다 — 이때는 `dropoff_count` 전부가 그 세그먼트로 간다.
   zone당 세그먼트 수가 적어(평균 약 74개) zone별로 작은 공간 인덱스를 만들어도 충분히
   빠르다.
5. **집계**: 세그먼트별로 배분된 `dropoff_count`(반경 매칭이면 분수, fallback이면
   정수)를 합산해 `segment_hotspot_count`를 만든다. `map_zone_segment.parquet`의
   (segment_id, zone_id) 전체에 left join해서, 매칭이 0건인 세그먼트도
   `segment_hotspot_count = 0`으로 명시적으로 포함시킨다 (이렇게 해야 다음 단계의
   zone 합=1 정규화가 zone에 속한 세그먼트 전부를 커버한다).
6. **스무딩 + 정규화**:
   ```
   spatial_weight(seg) = (segment_hotspot_count(seg) + α) / Σ_{s ∈ zone(seg)} (segment_hotspot_count(s) + α)
   ```
   `α`(라플라스 스무딩 상수)는 구현 단계에서 실측(예: zone별 평균
   `segment_hotspot_count`의 일정 비율)으로 정한다 — 지금은 "0건 세그먼트도 완전히
   0이 되면 안 된다"는 정성적 요구만 반영한 초안(TODO, `HOP_DECAY`처럼 팀 검토
   필요)이다.
7. **검증**:
   - `segment_id`는 `map_zone_segment.parquet`과 1:1로 일치 (누락/중복 없음)
   - zone별 `spatial_weight` 합이 1에 근접 (부동소수 오차 허용)
   - `spatial_weight` ∈ (0, 1]
   - `segment_hotspot_count` ≥ 0

## Gold 통합 (`src/tlc/gold.py`)

`_expand_zone_to_segment_hour`를 다음과 같이 바꾼다:

- 기존: `dropoff_count_raw = dropoff_count` (zone 총합, 세그먼트 전부 동일)
- 변경: `dropoff_count_raw = dropoff_count × spatial_weight(segment_id)`
- `map_segment_spatial_weight.parquet`을 `segment_id` 기준으로 merge해서 가져온다.
- hour와 무관하게 같은 `spatial_weight`를 24시간 전체에 곱한다.
- zone 총합이 그대로 유지됨을 확인: 한 zone 안에서 24시간 각각 `Σ_seg
  dropoff_count_raw(seg, hour) == dropoff_count(zone, hour)` (spatial_weight 합이
  1이므로 성립). `validate_dim_segment_tlc_volume`은 Gold 산출물(세그먼트x시간)만
  읽고 zone_id/원본 zone 총합을 갖고 있지 않아 이 불변식을 직접 검증할 수 없다 —
  대신 (1) `test_expand_zone_to_segment_hour_preserves_zone_total`(순수 함수
  단위테스트)와 (2) `validate_map_segment_spatial_weight`의 zone별 spatial_weight
  합=1 검증, 이 두 곳에서 실질적으로 보장한다. `build_dim_segment_tlc_volume`은
  추가로 spatial_weight 결측 비율이 `MAX_MISSING_SPATIAL_WEIGHT_FRACTION`(5%)을
  넘으면 하드 실패해서, 불변식이 깨질 만큼 결측이 쌓인 상태로 조용히 넘어가는
  것도 막는다.

## 알려진 한계 (TODO)

1. **2016년 공간 분포가 현재도 유효하다는 가정.** 검증 수단이 없다(TLC가 위경도
   제공을 끊었기 때문). 도로망/건물이 크게 안 바뀌었다는 전제에 의존한다.
2. **라플라스 스무딩 상수 `α`, buffer 반경(`HOTSPOT_SEGMENT_BUFFER_FT`=100ft), 거리
   역가중 epsilon(`HOTSPOT_INVERSE_DISTANCE_EPSILON_FT`=1.0ft)은 전부 정성적
   초안이다.** `HOP_DECAY`와 같은 성격의 TODO — "가까운 세그먼트가 더 받아야 한다",
   "0건 세그먼트가 완전히 0이 되면 안 된다"는 정성적 요구만 반영했고, 실측 검증된
   값은 아니다.
3. **zone 경계로 매칭을 한정**하기 때문에, zone 경계 바로 바깥에 실제로는 더 가까운
   세그먼트가 있어도 무시된다. zone 내부 비율의 합을 1로 유지하기 위한 trade-off다.
4. **하차(dropoff) 위치만 반영한다.** 승차(pickup) 위치는 이번 범위에 포함하지 않음
   (기존 `tlc_volume` 컴포넌트 자체가 dropoff 기준이라 일관성 유지).

## 참고 자료

- `docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md` — 기존
  `tlc_volume` 설계, 이번 설계가 수정하는 대상
- `src/mapping/zone_segment.py` — zone-segment 매핑에 쓴 STRtree/point-in-polygon
  패턴 재사용
- `src/mapping/ticketmaster_lion.py` — 반경(buffer) 내 전부 매칭 + 반경 밖이면
  최근접 1개 fallback 패턴 재사용 (여기선 buffer 안에서 거리 역가중 분배까지 추가)
- `src/taxi_zone/bronze.py` — 정적 참조 테이블 Bronze 적재 패턴
