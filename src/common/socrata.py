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

import concurrent.futures
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as ReqConnectionError,
    Timeout as ReqTimeout,
)
from urllib3.util.retry import Retry

from .config import (
    HTTP_TIMEOUT,
    SOCRATA_PAGE_HARD_TIMEOUT,
    SOCRATA_PAGE_SIZE,
)
from .logger import get_logger


logger = get_logger(__name__, log_to_file=True, log_file_stem="socrata")

# 페이지 요청을 별도 스레드에서 돌리고 SOCRATA_PAGE_HARD_TIMEOUT으로 하드
# 데드라인을 강제하기 위한 전용 executor. 데드라인을 넘겨 버려진 future의
# 스레드는 백그라운드에서 계속 대기하다가 결국 소켓이 끊기면 알아서 끝난다
# (Python은 스레드를 강제 종료할 수 없음) — max_workers를 여유 있게 둬서
# 그런 좀비 스레드가 몇 개 쌓여도 새 페이지 요청이 밀리지 않게 한다.
_page_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="socrata-page",
)


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

    session.get() 자체의 커넥션 재시도는 requests Retry(session에 mount된
    HTTPAdapter)가 처리하지만, 그건 "재시도 횟수"만 보장할 뿐 "전체 소요 시간"은
    못 막는다 — HTTP_TIMEOUT은 소켓 read 호출 한 번에만 걸리는 타임아웃이라,
    서버가 응답을 아주 느리게 찔끔찔끔 흘려보내면 각 read는 매번 타임아웃
    안쪽이라 안 걸리면서도 전체 요청은 시간제한 없이 계속 매달릴 수 있다
    (실제로 겪음 — 첫 페이지 요청이 1시간 넘게 안 끊기고 매달려 있다가 결국
    서버 쪽에서 RemoteDisconnected로 강제 종료함). 그래서 별도 스레드에서
    요청을 돌리고 SOCRATA_PAGE_HARD_TIMEOUT으로 전체 소요 시간에 하드
    데드라인을 강제한다 — 이 데드라인을 넘기면 그 결과는 버리고(스레드 자체는
    Python이 강제 종료할 수 없어 백그라운드에 남지만, 결국 소켓이 끊기면서
    스스로 끝난다) 새 세션으로 재시도한다. 재시도마다 새 세션을 쓰는 건,
    커넥션 풀에 남아있는 문제 있는 연결을 계속 재사용하지 않기 위함이다.
    """

    for attempt in range(max_retries):

        attempt_session = session if attempt == 0 else make_session()

        try:
            future = _page_executor.submit(
                attempt_session.get,
                url,
                params=params,
                timeout=HTTP_TIMEOUT,
            )

            res = future.result(timeout=SOCRATA_PAGE_HARD_TIMEOUT)

            res.raise_for_status()

            return res.json()

        except (
            ChunkedEncodingError,
            ReqConnectionError,
            ReqTimeout,
            concurrent.futures.TimeoutError,
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
                "Socrata 응답 중 연결 끊김/데드라인(%ds) 초과: "
                "retry=%d/%d wait=%ds error=%s",
                SOCRATA_PAGE_HARD_TIMEOUT,
                attempt + 1,
                max_retries,
                wait,
                e,
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


def _select_for_order(order_columns):
    """
    order에 :id 같은 시스템 컬럼이 섞여 있으면 $select로 명시해야 한다.

    실측 확인: $order=:id를 걸어도 $select에 명시하지 않으면 응답 JSON에
    :id 필드 자체가 안 내려온다. 그러면 커서가 다음 페이지 값을 못 뽑아서
    (None) 그 컬럼 기준 조건이 "col > null"이 되어 항상 거짓이 되고,
    조용히 첫 페이지 분량만 받고 끝나버린다 — 에러 없이 데이터가 잘려나가는
    가장 위험한 케이스. :로 시작하는 order 컬럼이 있으면 '*, :col, ...'로
    전체 필드 + 시스템 컬럼을 같이 요청해서 이 문제를 막는다.
    """

    system_columns = [c for c in order_columns if c.startswith(":")]

    if not system_columns:
        return None

    return "*, " + ", ".join(system_columns)


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
    select = _select_for_order(order_columns)

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
            if select:
                params["$select"] = select

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
    select = _select_for_order(order_columns)

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
        if select:
            params["$select"] = select

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