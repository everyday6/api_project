"""
Bronze 수집: NYC DOT Real-Time Traffic Speed Data

DOT 소스는 5분 간격으로 갱신되지만, 실제 공개 시점은 미국 동부 로컬시간
기준으로 몇 시간씩 지연될 수 있다. Airflow가 넘겨주는 실행 구간(UTC,
data_interval_start/end)을 그대로 시간창으로 써서 요청하면 그 구간에는
실제로 데이터가 존재하지 않아 매번 count=0으로 잡힌다(시간대 변환 +
지연 추정을 정확히 맞춰야 하는 문제). 대신 "마지막으로 성공적으로 수집한
data_as_of보다 새로운 것"만 매번 가져오는 방식으로 이 문제를 근본적으로
피한다 — 시간대/지연 추정이 전혀 필요 없다.

수집된 5분 단위 판독값은 Bronze에 개별 행으로 그대로 저장한다(정제/집계는
Silver1/Gold2에서).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from src.common.config import BRONZE_DIR, BUCKET_MINUTES, DATASETS
from src.common.logger import get_logger
from src.common.socrata import fetch_all, make_session
from src.common.utils import save_parquet
from src.lion.bronze import BRONZE_ROOT as LION_BRONZE_ROOT
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.silver2.segment_speed_match import match_links_to_segments
from src.speed import synthetic
from src.speed.bronze_validation import _validate_and_decide_df

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze")

SPEED_URL = DATASETS["speed"]
BRONZE_ROOT = BRONZE_DIR / "speed"

_MARKER_FILENAME = "_last_ingested_data_as_of.txt"

_NY_TZ = ZoneInfo("America/New_York")
# 마커가 아직 없을 때(최초 실행) 쓰는 하한선. 진짜 1970년부터로 잡으면
# NYC DOT 피드 전체 역사(실측 1억 행 이상)를 한 번에 끌어오려다 Airflow
# worker가 죽는다. 데이터 발행 지연(2~3시간)보다 넉넉하게 잡은 최근
# 시점이면 되고, 마커가 생긴 뒤로는 다시 안 쓰인다 - 정확한 시간대/지연
# 추정이 필요 없다는 이 모듈의 핵심 설계는 그대로 유지된다(대략적인
# 하한선이면 충분).
_BOOTSTRAP_LOOKBACK_HOURS = 6


def _bootstrap_marker() -> str:
    return (datetime.now(_NY_TZ) - timedelta(hours=_BOOTSTRAP_LOOKBACK_HOURS)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _marker_path(bronze_root):
    return bronze_root / _MARKER_FILENAME


def _read_marker(bronze_root) -> str | None:
    marker_path = _marker_path(bronze_root)
    if not marker_path.exists():
        return None
    return marker_path.read_text().strip()


def _write_marker(bronze_root, data_as_of: str) -> None:
    bronze_root.mkdir(parents=True, exist_ok=True)
    _marker_path(bronze_root).write_text(data_as_of)


def _new_data_where(marker: str | None) -> str:
    return f"data_as_of > '{marker or _bootstrap_marker()}'"


def _get_count(session, marker: str | None) -> int:
    """마커보다 새로운 행이 몇 개인지 $select=count(*)로 가볍게 확인한다."""
    response = session.get(
        SPEED_URL,
        params={"$select": "count(*)", "$where": _new_data_where(marker)},
        timeout=30,
    )
    response.raise_for_status()
    return int(response.json()[0]["count"])


def has_new_speed_data(bronze_root=BRONZE_ROOT) -> bool:
    """마커보다 새로운 판독값이 하나라도 있으면 True. short-circuit 태스크가
    쓴다. 이 함수는 마커를 절대 쓰지 않는다(읽기 전용) — 마커는 실제 수집이
    성공했을 때만 collect_speed_data()가 갱신한다. 체크 시점에 마커를 먼저
    갱신해버리면, 그 뒤 수집/처리 태스크가 실패해서 재시도할 때 "이미 처리한
    구간"으로 오인해 조용히 건너뛰게 된다."""

    marker = _read_marker(bronze_root)
    session = make_session()
    count = _get_count(session, marker)

    logger.info(f"[speed_bronze] marker={marker!r} 이후 판독값 count={count}")
    return count > 0


def _find_latest_lion_gdb(lion_bronze_root=LION_BRONZE_ROOT):
    version_dirs = sorted(
        p for p in lion_bronze_root.glob("version_date=*") if (p / "_metadata.txt").exists()
    )
    if not version_dirs:
        raise FileNotFoundError(f"{lion_bronze_root}에 완료된 LION Bronze 스냅샷이 없습니다")

    gdbs = list(version_dirs[-1].rglob("*.gdb"))
    if not gdbs:
        raise FileNotFoundError(f"{version_dirs[-1]} 안에 .gdb가 없습니다")
    return gdbs[0]


def _synthesize_uncovered_segments(links_df: pd.DataFrame, data_as_of: str) -> pd.DataFrame:
    """실제 속도 피드(고정 125개 link)가 커버 안 하는 LION routable
    세그먼트에 대해 synthetic speed row를 만든다(src/speed/synthetic.py
    참고) - routable 세그먼트의 92% 이상이 실제 피드와 매칭되는 link가
    근처에 없다."""

    if not DIM_SEGMENT_PATH.exists():
        logger.warning("[speed_bronze] dim_segment 없음 - synthetic 보강 스킵")
        return pd.DataFrame(columns=synthetic.SPEED_COLUMNS)

    dim_segment = pd.read_parquet(str(DIM_SEGMENT_PATH))
    routable = dim_segment[dim_segment["is_routable"]]

    matched = match_links_to_segments(links_df, routable)
    covered_ids = set(matched["segment_id"])
    uncovered_ids = set(routable["segment_id"]) - covered_ids

    # 참고표(geometry->link_points 변환 + POSTED_SPEED 조회, 세그먼트당
    # 무거운 계산)는 캐시가 있으면 그대로 읽는다 - LION은 분기에 한 번만
    # 바뀌는데 이 함수는 30분마다 불려서, 캐시 없이는 매번 gdb 원본을
    # 다시 읽게 된다(로드만 9초+).
    reference_table = synthetic.load_or_build_reference_table(
        dim_segment_loader=lambda: routable,
        posted_speed_loader=lambda: synthetic.load_posted_speed(_find_latest_lion_gdb()),
    )

    return synthetic.build_synthetic_rows(reference_table, uncovered_ids, data_as_of)


def collect_speed_data(bronze_root=BRONZE_ROOT) -> str:
    """마커보다 새로운 속도 판독값을 전부 받아, 저장하기 전에 검증하고
    통과한 경우에만 Bronze에 parquet으로 저장한 뒤 마커를 이번 배치의
    최댓값(data_as_of)으로 갱신한다.

    API 응답은 이 시점에 이미 메모리에 다 있으므로, 저장 후 다시 읽어서
    검증하는 대신 저장 직전에 바로 검증한다(critical 검증 실패시 저장
    자체를 하지 않는다 - 2026-08-26 순서 변경. TLC처럼 파일을 먼저
    "다운로드"해야만 하는 경우와 달리, speed는 API 응답이라 검증에 파일이
    필요 없다).

    결과가 0건이면 마커를 건드리지 않고 빈 문자열을 반환한다(정상 케이스 —
    상위 DAG가 short-circuit으로 이미 걸러내지만, 이 함수 자체도 방어적으로
    처리한다). critical 검증 실패도 마커를 안 건드리고 빈 문자열을
    반환한다 - 둘 다 "이번 사이클엔 유효한 새 데이터가 없다"는 같은
    결과라, collect_bronze 태스크 자체가 @task.short_circuit으로 뒤(Silver)
    실행 여부를 판단한다.
    """

    marker = _read_marker(bronze_root)
    rows = fetch_all(SPEED_URL, where=_new_data_where(marker), order="data_as_of")

    if not rows:
        logger.info(f"[speed_bronze] marker={marker!r} 이후 결과 없음")
        return ""

    df = pd.DataFrame(rows)
    max_data_as_of = str(df["data_as_of"].max())

    # 마커가 없거나 오래돼서(부트스트랩, 장애 복구 등) 한 번에 여러 30분
    # 버킷치가 몰려 들어오면, synthetic 보강을 배치 전체에 한 번만 하면 안
    # 된다 - 그러면 보강된 행이 전부 배치의 최신 시각(max_data_as_of) 하나로
    # 찍혀서, 그 버킷만 거의 다 채워지고 나머지 버킷은 실제 센서로 잡힌
    # segment만 남아 듬성듬성해진다(실제로 겪은 사고 - 2026-08-26). 30분
    # 버킷별로 나눠서 각자 자기 몫의 synthetic을 자기 시각으로 찍어야 모든
    # 버킷이 고르게 채워진다. 정상 상황(백로그 없이 한 사이클에 버킷 1개)
    # 에서는 그룹이 1개뿐이라 지금과 동일하게 한 번만 돈다.
    bucket = pd.to_datetime(df["data_as_of"]).dt.floor(f"{BUCKET_MINUTES}min")
    synthetic_frames = []
    for _, bucket_df in df.groupby(bucket):
        bucket_links_df = bucket_df.drop_duplicates("link_id")[["link_id", "link_points"]]
        bucket_max_data_as_of = str(bucket_df["data_as_of"].max())
        synthetic_frames.append(
            _synthesize_uncovered_segments(bucket_links_df, bucket_max_data_as_of)
        )

    synthetic_df = (
        pd.concat(synthetic_frames, ignore_index=True)
        if synthetic_frames
        else pd.DataFrame(columns=synthetic.SPEED_COLUMNS)
    )
    if not synthetic_df.empty:
        df = pd.concat([df, synthetic_df], ignore_index=True)

    if not _validate_and_decide_df(df, f"batch_end={max_data_as_of}, rows={len(df)}"):
        return ""

    out_path = save_parquet(
        df, bronze_root, f"batch_end={max_data_as_of.replace(':', '')}.parquet"
    )

    # parquet 저장이 성공한 뒤에만 마커를 갱신한다 — 저장이 실패하면 마커를
    # 건드리지 않아야 재시도가 이번 배치를 통째로 다시 수집할 수 있다.
    _write_marker(bronze_root, max_data_as_of)

    logger.info(f"[speed_bronze] {len(df)}행 저장 -> {out_path} (marker -> {max_data_as_of})")
    return str(out_path)
