"""
Bronze — TicketMaster Discovery API

역할

- NYC 공연/경기 등 사람이 모이는 이벤트 수집
- API 원본을 최대한 그대로 저장
- Parquet 저장을 위한 최소 구조 변환만 수행
- 날짜별 스냅샷 저장
- Daily DAG 실행일 기준 앞으로 120일 수집
- 기본 7일 단위로 조회
- 1,000건 초과 구간은 추가 분할하여 누락 방지

※ 중복 제거, 필터링, 날짜/시간 정제 등은
Silver에서 수행한다.
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import (
    BRONZE_DIR,
    HTTP_TIMEOUT,
    TICKETMASTER_API_KEY,
    TICKETMASTER_URL,
    TICKETMASTER_CITY,
    TICKETMASTER_PAGE_SIZE,
    TICKETMASTER_MAX_RESULTS,
    TICKETMASTER_SLEEP,
    TICKETMASTER_LOOKAHEAD_DAYS,
    TICKETMASTER_CHUNK_DAYS,
)
from common.utils import make_session, save_parquet
from common.logger import get_logger



logger = get_logger(__name__, log_to_file=True, log_file_stem="ticketmaster_bronze")

SOURCE = "ticketmaster"


def request_page(
    session,
    start_date,
    end_date,
    page=0,
):
    """Ticketmaster API의 특정 페이지를 조회한다."""

    params = {
        "apikey": TICKETMASTER_API_KEY,
        "city": TICKETMASTER_CITY,
        "startDateTime": f"{start_date}T00:00:00Z",
        "endDateTime": f"{end_date}T23:59:59Z",
        "size": TICKETMASTER_PAGE_SIZE,
        "page": page,
        "sort": "date,asc",
    }

    try:
        res = session.get(
            TICKETMASTER_URL,
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        res.raise_for_status()
    except requests.RequestException:
        logger.exception(
            "Ticketmaster 페이지 조회 실패: %s~%s page=%d",
            start_date,
            end_date,
            page,
        )
        raise

    return res.json()


def fetch_range(
    session,
    start_date,
    end_date,
):
    """
    지정 기간의 이벤트를 수집한다.

    1,000건 이하이면 페이지네이션하고,
    1,000건을 초과하면 기간을 절반으로 나눠 다시 수집한다.
    """

    first_page = request_page(
        session,
        start_date,
        end_date,
        page=0,
    )

    total = (
        first_page
        .get("page", {})
        .get("totalElements", 0)
    )

    # 1,000건 초과 → 기간 추가 분할
    if total > TICKETMASTER_MAX_RESULTS:

        if start_date == end_date:
            raise RuntimeError(
                f"{start_date}: 하루 이벤트가 "
                f"{total:,}건으로 "
                f"{TICKETMASTER_MAX_RESULTS:,}건을 초과합니다."
            )

        days = (end_date - start_date).days

        mid_date = (
            start_date
            + timedelta(days=days // 2)
        )

        logger.warning(
            "Ticketmaster 추가 분할: "
            "%s ~ %s total=%d",
            start_date,
            end_date,
            total,
        )

        left = fetch_range(
            session,
            start_date,
            mid_date,
        )

        right = fetch_range(
            session,
            mid_date + timedelta(days=1),
            end_date,
        )

        return left + right

    # 1,000건 이하 → 그대로 페이지네이션
    events = (
        first_page
        .get("_embedded", {})
        .get("events", [])
    )

    total_pages = (
        first_page
        .get("page", {})
        .get("totalPages", 1)
    )

    for page in range(
        1,
        total_pages,
    ):

        time.sleep(TICKETMASTER_SLEEP)

        data = request_page(
            session,
            start_date,
            end_date,
            page=page,
        )

        batch = (
            data
            .get("_embedded", {})
            .get("events", [])
        )

        events.extend(batch)

    return events


def fetch_all_events(
    session,
    start_date,
    end_date,
):
    """전체 조회 기간을 기본 7일 단위로 나눠 수집한다."""

    events = []

    current = start_date

    while current <= end_date:

        chunk_end = min(
            current
            + timedelta(days=TICKETMASTER_CHUNK_DAYS - 1),
            end_date,
        )

        chunk_events = fetch_range(
            session,
            current,
            chunk_end,
        )

        events.extend(
            chunk_events
        )

        logger.info(
            "Ticketmaster 기간 수집: "
            "%s ~ %s rows=%d total=%d",
            current,
            chunk_end,
            len(chunk_events),
            len(events),
        )

        current = (
            chunk_end
            + timedelta(days=1)
        )

        time.sleep(TICKETMASTER_SLEEP)

    return events


def flatten(events):
    """
    중첩 JSON을 Parquet으로 저장할 수 있도록 평탄화한다.
    list/dict 값은 JSON 문자열로 저장한다.
    """

    df = pd.json_normalize(
        events,
        sep="_",
    )

    for col in df.columns:

        if df[col].apply(
            lambda v: isinstance(
                v,
                (list, dict),
            )
        ).any():

            df[col] = df[col].apply(
                lambda v: (
                    json.dumps(
                        v,
                        ensure_ascii=False,
                    )
                    if isinstance(
                        v,
                        (list, dict),
                    )
                    else v
                )
            )

    return df


def build() -> str:
    """fetch -> save만 한다(validate 없음)."""

    if not TICKETMASTER_API_KEY:
        raise ValueError(
            "TICKETMASTER_API_KEY가 없습니다."
        )

    run_date = os.getenv(
        "RUN_DATE",
        date.today().isoformat(),
    )

    start_date = date.fromisoformat(
        run_date
    )

    end_date = (
        start_date
        + timedelta(
            days=TICKETMASTER_LOOKAHEAD_DAYS
        )
    )

    out_dir = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
    )

    logger.info(
        "Ticketmaster 수집 시작: "
        "start=%s end=%s",
        start_date,
        end_date,
    )

    session = make_session()

    all_events = fetch_all_events(
        session,
        start_date,
        end_date,
    )

    if not all_events:
        raise ValueError(
            "Ticketmaster 받은 데이터가 없습니다."
        )

    # Parquet 저장을 위한 최소 변환
    df = flatten(
        all_events
    )

    path = save_parquet(
        df,
        out_dir,
    )

    logger.info(
        "Ticketmaster 수집 빌드 완료: "
        "rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """저장된 Bronze 파일에 행이 실제로 있는지 확인한다."""
    df = pd.read_parquet(str(path))
    if df.empty:
        raise ValueError("Ticketmaster 받은 데이터가 없습니다.")
    return path


def main() -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build()
    validate_output(path)
    return path


if __name__ == "__main__":
    main()