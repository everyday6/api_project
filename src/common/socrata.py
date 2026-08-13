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

페이지네이션은 $offset이 아닌 keyset(커서) 방식을 사용한다.
$offset은 값이 커질수록 Socrata 백엔드가 앞부분을 전부 스캔하고
버려야 해서 뒤로 갈수록 응답이 느려지고 타임아웃이 잦아진다.
keyset 방식은 "직전 마지막 행의 정렬 컬럼 값보다 큰 행"을 조건으로
걸기 때문에 몇 백만 번째 페이지든 응답 속도가 거의 일정하다.
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
    HTTP_TIMEOUT,
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
                timeout=HTTP_TIMEOUT,
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


# ==========================
# keyset(커서) 페이지네이션 헬퍼
# ==========================

def _parse_order_columns(order):
    """
    '$order' 문자열에서 실제 컬럼명만 추출한다.

    예: "permitnumber" -> ["permitnumber"]
        "permitnumber, :id" -> ["permitnumber", ":id"]
        "permitnumber DESC" -> ["permitnumber"] (방향은 무시, 오름차순 가정)
    """

    columns = []

    for part in order.split(","):
        token = part.strip().split()[0]
        columns.append(token)

    return columns


def _format_cursor_value(value):
    """
    SoQL 리터럴로 안전하게 변환한다.

    - 문자열: 작은따옴표 escape 후 quoting
    - 숫자/불리언: 그대로
    - None: null (이 경우 해당 컬럼은 order 기준으로 부적합하므로
      실제로는 거의 발생하지 않아야 함)
    """

    if value is None:
        return "null"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _build_cursor_where(order_columns, cursor_values):
    """
    직전 페이지의 마지막 행보다 '뒤에 오는' 행만 걸러내는 SoQL 조건 생성.

    order_columns가 여러 개인 경우 튜플 비교와 동일하게 동작한다.
    예: order_columns=[a, b], cursor_values=[v1, v2] 이면

        (a > v1) OR (a = v1 AND b > v2)
    """

    if cursor_values is None:
        return None

    clauses = []

    for i, col in enumerate(order_columns):

        eq_parts = [
            f"{order_columns[j]} = {_format_cursor_value(cursor_values[j])}"
            for j in range(i)
        ]

        gt_part = f"{col} > {_format_cursor_value(cursor_values[i])}"

        clauses.append(
            "(" + " AND ".join(eq_parts + [gt_part]) + ")"
        )

    return "(" + " OR ".join(clauses) + ")"


def _combine_where(base_where, cursor_where):

    if cursor_where is None:
        return base_where

    return f"({base_where}) AND {cursor_where}"


def fetch_all_streaming(
    url,
    where,
    order,
    out_path,
):
    """
    Socrata 데이터를 parquet으로 스트리밍 저장한다.

    대용량 데이터용. offset이 아닌 keyset 커서로 페이지네이션한다.
    """

    session = make_session()
    order_columns = _parse_order_columns(order)

    cursor_values = None
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

            cursor_where = _build_cursor_where(
                order_columns,
                cursor_values,
            )

            params = {
                "$where": _combine_where(where, cursor_where),
                "$limit": SOCRATA_PAGE_SIZE,
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

            # 다음 페이지를 위한 커서 갱신
            last_row = batch[-1]
            cursor_values = [
                last_row.get(col)
                for col in order_columns
            ]

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
    order_columns = _parse_order_columns(order)

    cursor_values = None
    rows = []

    while True:

        cursor_where = _build_cursor_where(
            order_columns,
            cursor_values,
        )

        params = {
            "$where": _combine_where(where, cursor_where),
            "$limit": SOCRATA_PAGE_SIZE,
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

        last_row = batch[-1]
        cursor_values = [
            last_row.get(col)
            for col in order_columns
        ]

    logger.info(
        "Socrata fetch completed: rows=%d",
        len(rows),
    )

    return rows