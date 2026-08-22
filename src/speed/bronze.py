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

import pandas as pd

from src.common.config import BRONZE_DIR, DATASETS
from src.common.logger import get_logger
from src.common.socrata import fetch_all, make_session

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze")

SPEED_URL = DATASETS["speed"]
BRONZE_ROOT = BRONZE_DIR / "speed"

_MARKER_FILENAME = "_last_ingested_data_as_of.txt"
# 마커가 없을 때(최초 실행)도 where절이 항상 유효한 문자열이 되도록 쓰는
# 더미 하한선. socrata.fetch_all의 페이지네이션(_combine_where)이 where=None을
# 커서 조건과 결합할 때 문자열 포맷이 깨지므로, None 대신 항상 실제 비교식을
# 넘긴다.
_EPOCH_SENTINEL = "1970-01-01T00:00:00"


def _marker_path(bronze_root):
    return bronze_root / _MARKER_FILENAME


def _read_marker(bronze_root) -> str | None:
    marker_path = _marker_path(bronze_root)
    if not marker_path.exists():
        return None
    return marker_path.read_text()


def _write_marker(bronze_root, data_as_of: str) -> None:
    bronze_root.mkdir(parents=True, exist_ok=True)
    _marker_path(bronze_root).write_text(data_as_of)


def _new_data_where(marker: str | None) -> str:
    return f"data_as_of > '{marker or _EPOCH_SENTINEL}'"


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


def collect_speed_data(bronze_root=BRONZE_ROOT) -> str:
    """마커보다 새로운 속도 판독값을 전부 받아 Bronze에 parquet으로 저장하고,
    저장에 성공한 경우에만 마커를 이번 배치의 최댓값(data_as_of)으로
    갱신한다.

    결과가 0건이면 마커를 건드리지 않고 빈 문자열을 반환한다(정상 케이스 —
    상위 DAG가 short-circuit으로 이미 걸러내지만, 이 함수 자체도 방어적으로
    처리한다).
    """

    marker = _read_marker(bronze_root)
    rows = fetch_all(SPEED_URL, where=_new_data_where(marker), order="data_as_of")

    if not rows:
        logger.info(f"[speed_bronze] marker={marker!r} 이후 결과 없음")
        return ""

    df = pd.DataFrame(rows)
    max_data_as_of = str(df["data_as_of"].max())

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / f"batch_end={max_data_as_of.replace(':', '')}.parquet"
    df.to_parquet(str(out_path), index=False)

    # parquet 저장이 성공한 뒤에만 마커를 갱신한다 — 저장이 실패하면 마커를
    # 건드리지 않아야 재시도가 이번 배치를 통째로 다시 수집할 수 있다.
    _write_marker(bronze_root, max_data_as_of)

    logger.info(f"[speed_bronze] {len(df)}행 저장 -> {out_path} (marker -> {max_data_as_of})")
    return str(out_path)
