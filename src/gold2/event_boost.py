"""
Gold: event + ticketmaster 진앙 segment을 합쳐서 segment x hour별
event_boost(수요 가산치)를 계산한다.

closure_penalty.py와 대칭 구조다 — 저긴 공사/통제가 용량을 깎고, 여긴 행사가
수요를 올린다. 홉 감쇠(spread_with_decay)는 진앙이 공사든 행사든 다를 이유가
없어서 closure_penalty.py 걸 그대로 재사용한다.

설계:
1. "진앙" 레코드 = event_lion(도로 통제형 행사, mapping_status=="matched"만)
   + ticketmaster Gold1(공연장, 서비스 대상 필터 완료, segment_id 있는 행 전부) 둘
   다 합쳐서 만든다.
   - event는 closure_type(차량 통행에 얼마나 방해되는지)별로 가중치를 다르게
     준다 — Full Street Closure/Pedestrian Plaza(차량 아예 못 지나감)=1.0,
     Sidewalk and Curb Lane Closure(일부만 막힘)=0.5, Curb Lane Only=0.25.
   - ticketmaster는 신뢰 가능한 관객 수·수용 인원 데이터가 없어 행사마다
     동일한 보수적 가중치를 사용한다. end_ts가 있는 경우가 거의 없어서(실측 약 2%), 없으면
     start_ts부터 TICKETMASTER_DEFAULT_DURATION_HOURS 동안 활성이라고 가정한다.

2. event/ticketmaster는 시작/종료가 구체적 타임스탬프라(공사 permit처럼
   "몇 시~몇 시, 무슨 요일"식 반복 패턴이 아니라 그 날짜/시각에 한 번 열리는
   것) closure_penalty의 hour/day mask 같은 별도 분해 없이, query_date의 각
   시각을 타임스탬프로 만들어 [start_ts, end_ts) 안에 있는지만 확인한다.

3. graph_segment_adjacency로 최대 3홉까지 closure_penalty.spread_with_decay()를
   그대로 재사용해서 퍼뜨린다.

4. 누적된 강도를 0~1 사이 수요 가산치로 변환한다: event_boost =
   intensity / (intensity + K) — closure_penalty와 같은 포화 곡선. 행사가
   전혀 없는 segment는 정확히 0이어야 해서(percentile 정규화를 쓰면 "행사
   없음"도 전체 분포 중간쯤 순위를 받아버려 부적절) 이 방식을 쓴다.

결과 스키마: segment_id, hour(0~23), event_intensity, event_boost(0~1).
"""

from __future__ import annotations

import pandas as pd

from src.common.config import GOLD1_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.gold2.closure_penalty import spread_with_decay

logger = get_logger(__name__, log_to_file=True, log_file_stem="event_boost")

MAP_EVENT_LION_DIR = SILVER2_DIR / "event_lion"
TICKETMASTER_GOLD1_DIR = GOLD1_DIR / "ticketmaster"

# RDS 서빙 테이블 이름(dt 컬럼으로 파티션 흉내) — S3의 위 두 디렉터리와
# 같은 데이터를 들고 있다. 서빙 API는 이쪽에서 읽는다.
MAP_EVENT_LION_TABLE = "map_event_lion"
MAP_TICKETMASTER_LION_TABLE = "map_ticketmaster_lion"

# TODO(팀 검토 필요): 근거 없는 초안 — closure_type별 "차량 통행에 얼마나
# 방해되는가"를 정성적으로 매긴 가중치.
EVENT_CLOSURE_TYPE_WEIGHT = {
    "Full Street Closure": 1.0,
    "Pedestrian Plaza": 1.0,
    "Sidewalk and Curb Lane Closure": 0.5,
    "Curb Lane Only": 0.25,
}
EVENT_CLOSURE_TYPE_DEFAULT_WEIGHT = 0.5  # 위 목록에 없는 값이 나오면 중간값으로

# Ticketmaster에는 신뢰 가능한 실제 관객 수가 없으므로 모든 행에 동일하게
# 적용하는 보수적 가중치. venue capacity 프록시는 검증된 수집 파이프라인이
# 마련되기 전까지 점수에 사용하지 않는다.
TICKETMASTER_WEIGHT = 0.5

# TODO(팀 검토 필요): Ticketmaster는 end_ts가 거의 없어서(실측 약 98% 결측)
# 공연 평균 길이를 가정한 초안.
TICKETMASTER_DEFAULT_DURATION_HOURS = 3

# TODO(팀 검토 필요): closure_penalty의 HALF_SATURATION_INTENSITY와 같은
# 성격의 초안 — event_boost = intensity/(intensity+K) 곡선의 K.
EVENT_HALF_SATURATION_INTENSITY = 1.5


def load_ground_zero_records(event_mapping_dt: str, ticketmaster_gold1_dt: str) -> pd.DataFrame:
    """
    event_lion Silver2 + ticketmaster Gold1 결과를 합쳐 segment_id x start_ts x
    end_ts x weight 레코드로 만든다. weight는 그 행이 활성일 때 이 segment에
    기여하는 강도(진앙 intensity 집계의 기본 단위).
    """
    events = db.read_partition(
        MAP_EVENT_LION_TABLE,
        event_mapping_dt,
        columns=["segment_id", "start_ts", "end_ts", "closure_type", "mapping_status"],
    )
    events = events[(events["mapping_status"] == "matched") & events["segment_id"].notna()]
    events = events.copy()
    events["weight"] = events["closure_type"].map(EVENT_CLOSURE_TYPE_WEIGHT).fillna(EVENT_CLOSURE_TYPE_DEFAULT_WEIGHT)
    events = events[["segment_id", "start_ts", "end_ts", "weight"]]

    tm_path = TICKETMASTER_GOLD1_DIR / f"dt={ticketmaster_gold1_dt}" / "data.parquet"
    tm = pd.read_parquet(tm_path, columns=["segment_id", "start_ts", "end_ts"])
    tm = tm[tm["segment_id"].notna() & tm["start_ts"].notna()].copy()
    # end_ts가 없으면(대부분) start_ts + 기본 지속시간으로 가정한다.
    fallback_end = tm["start_ts"] + pd.Timedelta(hours=TICKETMASTER_DEFAULT_DURATION_HOURS)
    tm["end_ts"] = tm["end_ts"].fillna(fallback_end)
    tm["weight"] = TICKETMASTER_WEIGHT
    tm = tm[["segment_id", "start_ts", "end_ts", "weight"]]

    combined = pd.concat([events, tm], ignore_index=True)
    logger.info(
        "event_boost 진앙 레코드: event=%d건, ticketmaster=%d건, 합쳐서=%d건 (고유 segment=%d개)",
        len(events), len(tm), len(combined), combined["segment_id"].nunique(),
    )
    return combined


def _active_mask(ts: pd.Timestamp, start: pd.Series, end: pd.Series) -> pd.Series:
    """ts가 [start, end) 안에 있는지 — event/ticketmaster는 시각 자체가 구체적
    타임스탬프라 closure_penalty처럼 시각/요일을 따로 분해해서 볼 필요 없이
    직접 비교한다."""
    return (start <= ts) & (ts < end)


def compute_hourly_boost(
    records: pd.DataFrame,
    query_date: str,
    adjacency: dict,
    half_saturation: float = EVENT_HALF_SATURATION_INTENSITY,
) -> pd.DataFrame:
    """query_date의 0~23시 각각에 대해 그 시각에 활성인 event/ticketmaster만으로
    감쇠/합산/포화변환을 계산한다."""
    frames = []

    for hour in range(24):
        ts = pd.Timestamp(f"{query_date} {hour:02d}:00:00")
        active = records[_active_mask(ts, records["start_ts"], records["end_ts"])]
        if active.empty:
            continue

        intensity = active.groupby("segment_id")["weight"].sum()
        accum = spread_with_decay(intensity, adjacency)
        if not accum:
            continue

        rows = [
            {
                "segment_id": seg_id,
                "event_intensity": val,
                "event_boost": val / (val + half_saturation),
                "hour": hour,
            }
            for seg_id, val in accum.items()
        ]
        frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame(columns=["segment_id", "event_intensity", "event_boost", "hour"])

    return pd.concat(frames, ignore_index=True)


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("event_boost 결과가 비었습니다 — 해당 날짜에 활성 event/ticketmaster가 없을 수 있습니다.")
        return

    if not df["event_boost"].between(0, 1).all():
        raise ValueError("event_boost 값이 0~1 범위를 벗어났습니다.")

    if not df["hour"].between(0, 23).all():
        raise ValueError("hour 값이 0~23 범위를 벗어난 행이 있습니다.")

    logger.info(
        "event_boost 검증 완료: 행=%d(segment x hour), 영향받는 고유 segment=%d개, 평균=%.4f, 최대=%.4f",
        len(df), df["segment_id"].nunique(), df["event_boost"].mean(), df["event_boost"].max(),
    )
