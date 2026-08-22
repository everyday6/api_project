"""NYC Open Data(Socrata) 조회에 사용하는 최소 공통 클라이언트."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common.config import HTTP_TIMEOUT, SOCRATA_PAGE_SIZE, USER_AGENT
from src.common.logger import get_logger


logger = get_logger(__name__, log_to_file=True, log_file_stem="socrata")


def make_session() -> requests.Session:
    """일시적인 네트워크 오류와 서버 오류를 자동 재시도하는 세션을 만든다."""

    session = requests.Session()
    session.headers.update(USER_AGENT)
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_all(url: str, where: str, order: str) -> list[dict]:
    """조건에 맞는 Socrata 행을 페이지 단위로 끝까지 조회한다."""

    session = make_session()
    rows: list[dict] = []
    offset = 0

    while True:
        response = session.get(
            url,
            params={
                "$where": where,
                "$limit": SOCRATA_PAGE_SIZE,
                "$offset": offset,
                "$order": order,
            },
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        rows.extend(batch)
        logger.info("Socrata 조회 진행: rows=%s", len(rows))

        if len(batch) < SOCRATA_PAGE_SIZE:
            break
        offset += SOCRATA_PAGE_SIZE

    logger.info("Socrata 조회 완료: rows=%s", len(rows))
    return rows
