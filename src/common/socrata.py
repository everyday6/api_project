"""
Socrata API 공통 모듈

NYC Open Data는 전부 Socrata를 사용한다.
공사 · 통제 · 행사가 같은 방식이라 공통 모듈로 묶는다.

대용량 소스(공사 380만 건)는 pyarrow ParquetWriter로 스트리밍 저장한다.

- 전체를 메모리에 들고 있지 않음
- 한 페이지(5만 건)만 메모리에 존재
- 조각 파일 없이 최종 parquet 1개로 저장
- 응답 도중 연결이 끊겨도 재시도
- 실패 시 임시 파일 삭제
- 0건은 정상 케이스로 처리
"""

import time
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as ReqConnectionError,
)
from urllib3.util.retry import Retry

from common.config import (
    REQUEST_TIMEOUT,
    SOCRATA_PAGE_SIZE,
)


logger = logging.getLogger(__name__)


def make_session():
    """네트워크 오류 시 자동 재시도하는 세션."""

    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=["GET"],
    )

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )

    return session


def _get_page(
    session,
    url,
    params,
    max_retries=5,
):
    """
    Socrata API 한 페이지를 받아온다.

    연결 자체의 재시도는 requests Retry가 처리하고,
    응답을 받는 도중 끊기는 경우는 여기서 별도로 재시도한다.
    """

    for attempt in range(max_retries):

        try:
            res = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            res.raise_for_status()

            return res.json()

        except (
            ChunkedEncodingError,
            ReqConnectionError,
        ) as e:

            if attempt == max_retries - 1:
                logger.error(
                    "Socrata 응답 수신 실패: "
                    "attempts=%d error=%s",
                    max_retries,
                    e,
                )
                raise

            wait = 2 ** attempt

            logger.warning(
                "Socrata 응답 중 연결 끊김: "
                "retry=%d/%d wait=%ds",
                attempt + 1,
                max_retries,
                wait,
            )

            time.sleep(wait)


def fetch_all_streaming(
    url,
    where,
    order,
    out_path,
):
    """
    Socrata 데이터를 parquet으로 스트리밍 저장한다.

    대용량 데이터용.
    """

    session = make_session()

    offset = 0
    total = 0

    writer = None
    schema = None

    tmp_path = (
        out_path.parent
        / f"_tmp_{out_path.name}"
    )

    out_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 이전 실패에서 남은 임시 파일 제거
    if tmp_path.exists():
        tmp_path.unlink()

    try:

        while True:

            params = {
                "$where": where,
                "$limit": SOCRATA_PAGE_SIZE,
                "$offset": offset,
                "$order": order,
            }

            batch = _get_page(
                session,
                url,
                params,
            )

            if not batch:
                break

            table = pa.Table.from_pandas(
                pd.DataFrame(batch),
                preserve_index=False,
            )

            if writer is None:

                schema = table.schema

                writer = pq.ParquetWriter(
                    tmp_path,
                    schema,
                )

            else:

                # 해당 페이지에서 빠진 컬럼은 null로 채움
                for name in schema.names:

                    if name not in table.column_names:

                        table = table.append_column(
                            name,
                            pa.nulls(
                                table.num_rows,
                                type=schema.field(name).type,
                            ),
                        )

                table = (
                    table
                    .select(schema.names)
                    .cast(schema)
                )

            writer.write_table(
                table
            )

            total += len(batch)

            logger.info(
                "Socrata streaming progress: rows=%d",
                total,
            )

            offset += SOCRATA_PAGE_SIZE

    except Exception:

        logger.exception(
            "Socrata streaming failed: "
            "rows=%d tmp_path=%s",
            total,
            tmp_path,
        )

        # writer가 열려 있으면 먼저 닫음
        if writer is not None:
            writer.close()
            writer = None

        # 실패한 임시 파일 제거
        if tmp_path.exists():
            tmp_path.unlink()

        raise

    finally:

        if writer is not None:
            writer.close()

    # 결과가 0건이면 정상 종료
    if total == 0:

        logger.info(
            "Socrata result empty: no rows"
        )

        return 0

    # 전체 수집이 성공한 경우에만 최종 파일로 교체
    tmp_path.replace(
        out_path
    )

    logger.info(
        "Socrata streaming completed: "
        "rows=%d path=%s",
        total,
        out_path,
    )

    return total


def fetch_all(
    url,
    where,
    order,
):
    """
    작은 데이터셋용.

    행사 등 전체를 메모리에 올려도
    문제가 없는 데이터에 사용한다.
    """

    session = make_session()

    rows = []
    offset = 0

    while True:

        params = {
            "$where": where,
            "$limit": SOCRATA_PAGE_SIZE,
            "$offset": offset,
            "$order": order,
        }

        batch = _get_page(
            session,
            url,
            params,
        )

        if not batch:
            break

        rows.extend(
            batch
        )

        logger.info(
            "Socrata fetch progress: rows=%d",
            len(rows),
        )

        offset += SOCRATA_PAGE_SIZE

    logger.info(
        "Socrata fetch completed: rows=%d",
        len(rows),
    )

    return rows