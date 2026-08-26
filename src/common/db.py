"""
RDS(PostgreSQL) 공용 접근 헬퍼

psycopg2 저수준 쿼리만 감싼다. fallback 체인 같은 비즈니스 로직은 여기 두지
않는다 — 서빙 조회(src/serving/nav_lookup.py 등)가 이 모듈의
batch_get_items()를 호출해서 "없는 키는 결과에 없다"는 사실 자체를 fallback
트리거로 쓴다.

테이블마다 스키마와 기본키가 다르다(type1은 segment_id/record_type/bucket,
type2/3/4는 segment_id) — 이 모듈은 그 차이를 몰라도 되게 설계했다:

- ensure_table(table_name, columns, key_columns)만 "이 테이블에 어떤 컬럼과
  기본키가 있어야 하는지"를 호출부로부터 명시적으로 받는다. 배포
  시점에 한 번 부르는 함수라 스키마를 아는 게 자연스럽다.
- batch_write_items()는 반대로 스키마를 미리 몰라도 된다 — 넘어온 아이템
  dict들의 키 자체가 곧 그 배치에 쓸 컬럼 목록이다(type1처럼 버킷 행엔
  collected_date만, AVG 행엔 sample_count만 있는 식으로 필드가 달라도,
  없는 컬럼은 그 행에서 NULL로 채운다).
- batch_get_items()는 SELECT *로 읽고 실제 컬럼명을 그대로 dict 키로
  돌려준다 — 테이블이 어떤 컬럼을 가졌든 따라간다.

batch_write_items()는 upsert만 하고 삭제는 하지 않는다 — 호출부가 더 이상
안 보내는 기존 행은 그대로 남는다. "이번 호출 = 그 시점의 완전한 정답
집합"인 전체 스냅샷 쓰기(길이/통행료처럼 매번 대상 전체를 다시 계산하는
경우)는 replace_table_snapshot()을 쓴다 — staging 테이블에 적재 후
원자적으로 테이블을 통째로 바꿔치기해서, 이번에 없는 행은 스왑과 함께
자연히 사라진다(src/tlc/gold2.py의 Type3 스왑과 같은 원리를 축소한
버전). 반대로 매번 일부만 증분 upsert하는 경우(시간처럼)는 전체를
다시 쓸 수 없으니, cleanup_keys_not_in()으로 "지금 유효한 키 집합"과
실제 테이블을 직접 비교해 유효하지 않은 행만 지운다 — 과거 실행 이력에
의존하지 않는 멱등 연산이라 반복 호출해도 항상 같은 결과로 수렴한다.

배치 조회/쓰기 시 예외를 삼키지 않고 그대로 던진다 — 호출부(서빙 API)가
그 예외를 잡아서 fallback으로 넘어간다. 유일한 예외는 get_value()의
"테이블 자체가 아직 없음" 케이스뿐이다.
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from src.common.config import RDS_DB, RDS_HOST, RDS_PASSWORD, RDS_PORT, RDS_USER
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="db")

# 테이블명/컬럼명은 항상 코드 상수(config.py의 SERVING_TABLE_TYPE* 등)에서만
# 온다 - 그래도 psycopg2.sql.Identifier로 넘기기 전에 한 번 더 방어한다
# (식별자는 execute()의 파라미터 바인딩으로 이스케이프할 수 없어서, 문자열
# 조합 전에 형식을 검증해야 한다).
_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_-]*$")

# 배치 조회 시 복합키 IN (...)에 한 번에 넣을 키 개수. RDS
# 자체 한계보다 훨씬 보수적으로 잡아 쿼리 파라미터가 과도하게 커지는 걸 막는다.
_BATCH_GET_CHUNK = 1000

# 프로세스 안에서 재사용하는 지연 생성 커넥션. Lambda 웜스타트 사이에도
# 재사용되고, PgBouncer/RDS Proxy가 앞단에 있다는 전제로 커넥션 풀링
# 자체는 이 모듈이 아니라 인프라 레이어에 맡긴다.
_connection = None


def _dsn(*, connect_timeout: int = 1, statement_timeout_ms: int | None = None) -> dict:
    if not RDS_HOST or not RDS_DB:
        raise RuntimeError("RDS_HOST/RDS_DB 환경변수가 필요합니다")
    dsn = dict(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        connect_timeout=connect_timeout,
    )
    if statement_timeout_ms is not None:
        # 세션 옵션으로 서버 쪽에 강제한다 - psycopg2 자체엔 쿼리 실행 시간
        # 상한을 거는 클라이언트 옵션이 없다(src/serving/api.py의 Type3
        # 커넥션과 동일한 방식).
        dsn["options"] = f"-c statement_timeout={statement_timeout_ms}"
    return dsn


def new_connection(*, connect_timeout: int = 1, statement_timeout_ms: int | None = None):
    """항상 새 커넥션을 연다. 커넥션을 공유하면 안 되는 호출부가 명시적으로
    쓴다 — 예: src/tlc/gold2.py의 Type3 Spark 파티션별 쓰기 스레드는 스레드마다
    자기만의 커넥션이 있어야 한다(psycopg2 커넥션은 스레드 간 동시 공유가
    안전하지 않다).

    connect_timeout 기본값은 1초다 - "TCP 연결을 맺는" 단계는 읽기든
    쓰기든 같은 VPC 안에서는 항상 빨라야 정상이라, 배치 쓰기 쪽에도
    안전하게 걸 수 있다(연결 자체가 1초 넘게 안 되면 RDS 쪽 진짜 문제로
    보는 게 맞다). 반대로 statement_timeout_ms는 기본값이 무제한이다 -
    대량 upsert 같은 배치 쓰기는 "쿼리 실행 시간"이 1초를 넘는 게 정상이라,
    여기 짧은 값을 걸면 정상 동작까지 실패로 처리된다. 서빙 조회처럼
    "쿼리 자체도 느리면 곧장 실패해서 fallback으로 넘어가야 하는" 호출부만
    이 값을 명시적으로 넘긴다(src/serving/nav_lookup.py 참고).

    autocommit=True로 열어서 트랜잭션 개념 자체를 없앤다 - 문장 하나가
    실패해도 "트랜잭션 중단 상태"에 안 빠지고 다음 호출을 바로 이어갈 수 있다.
    """
    conn = psycopg2.connect(
        **_dsn(connect_timeout=connect_timeout, statement_timeout_ms=statement_timeout_ms)
    )
    conn.autocommit = True
    return conn


def _get_connection():
    """공유 커넥션을 재사용한다. 끊어졌으면(RDS 재부팅, 유휴 타임아웃 등)
    자동으로 다시 연다 - 첫 호출이 예외 대신 재연결로 복구되게 하기 위함."""
    global _connection
    if _connection is not None and not _connection.closed:
        try:
            with _connection.cursor() as cur:
                cur.execute("SELECT 1")
            return _connection
        except psycopg2.Error:
            logger.warning("공유 DB 커넥션이 끊어져 재연결합니다")
    _connection = new_connection()
    return _connection


def get_shared_connection():
    """공유 커넥션을 그대로 노출한다. batch_get_items/batch_write_items는
    "정확한 키로 단건/여러 건 조회"만 표현할 수 있는데, 부분 키 조회(예:
    segment_id만으로 그 세그먼트의 모든 time 행을 가져오는 것) 같은 커스텀
    쿼리가 필요한 호출부(src/serving/nav_lookup.py의 type1 배치 조회 참고)가
    이 함수로 커넥션만 빌려 쓴다. 재사용/재연결 동작은 내부 _get_connection과
    동일하다 - 별도 커넥션 풀을 새로 만들지 않는다."""
    return _get_connection()


def _validate_identifier(name: str) -> None:
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"잘못된 테이블/컬럼 이름입니다: {name}")


def _normalize_value(value):
    """DB가 돌려주는 값을 호출부가 다루기 편한 파이썬 기본형으로 되돌린다.

    - NUMERIC -> Decimal은 int/float으로(소비처가 Decimal을 몰라도 되게)
    - DATE/TIMESTAMP -> ISO 문자열로(기존 계약과 동일하게 유지)
    - JSONB -> psycopg2가 이미 dict/list로 파싱해서 주므로 그대로 둔다
    """
    if isinstance(value, Decimal):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def ensure_table(
    table_name: str,
    columns: dict[str, str],
    key_columns: tuple[str, ...] = ("segment_id",),
) -> None:
    """테이블이 없으면 만든다. 로컬 개발/테스트 편의용이다 - 운영 파이프라인
    코드 경로에서 매번 부르는 건 피하고, 배포 시점에
    scripts/create_rds_tables.py로 미리 만들어둔다.

    columns: 기본키를 제외한 {컬럼명: SQL 타입 선언}(updated_date 등 갱신
    시각도 이제 자동 관리 컬럼이 아니라 여기 columns에 명시적으로 넣는
    일반 컬럼이다 - 호출부가 값을 직접 채운다).
    key_columns: TEXT NOT NULL로 만들 기본키 컬럼 목록.
    """
    _validate_identifier(table_name)
    if not key_columns:
        raise ValueError("기본키 컬럼이 하나 이상 필요합니다")
    for name in (*key_columns, *columns):
        _validate_identifier(name)

    conn = _get_connection()
    key_defs = sql.SQL(", ").join(
        sql.SQL("{name} TEXT NOT NULL").format(name=sql.Identifier(name))
        for name in key_columns
    )
    value_defs = sql.SQL(", ").join(
        sql.SQL("{name} {decl}").format(name=sql.Identifier(name), decl=sql.SQL(decl))
        for name, decl in columns.items()
    )
    definition_parts = [key_defs]
    if columns:
        definition_parts.append(value_defs)
    all_defs = sql.SQL(", ").join(definition_parts)
    primary_key = sql.SQL(", ").join(sql.Identifier(name) for name in key_columns)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {table} (
                    {all_defs},
                    PRIMARY KEY ({primary_key})
                )
                """
            ).format(
                table=sql.Identifier(table_name),
                all_defs=all_defs,
                primary_key=primary_key,
            )
        )

        # CREATE TABLE IF NOT EXISTS는 이미 있던 테이블은 안 건드리므로,
        # 이 컬럼들이 생기기 전에 이미 만들어져 있던 테이블도 있을 수 있다.
        # ALTER로 한 번 더 보정한다. NOT NULL은 기존 행이 있으면 디폴트값
        # 없이 ALTER로 못 붙이므로, 이 경로에서는 제약 없이 타입만 추가한다.
        for name, decl in columns.items():
            bare_type = decl.split(" NOT NULL")[0].split(" DEFAULT")[0].strip()
            cur.execute(
                sql.SQL("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {decl}").format(
                    table=sql.Identifier(table_name),
                    name=sql.Identifier(name),
                    decl=sql.SQL(bare_type),
                )
            )


def _chunk(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def batch_get_items(table_name: str, keys: list[dict], *, conn=None) -> dict[tuple, dict]:
    """키 컬럼 dict 목록으로 여러 항목을 한 번에 조회한다.

    SELECT *로 읽어서 그 테이블이 실제로 가진 컬럼을 그대로 dict로 돌려준다
    (updated_at은 내부 관리용이라 제외). 반환값은 (기본키 값 tuple) -> item
    딕셔너리이며, 테이블에 없는 키는 결과에서 빠진다(호출부가 이걸로
    fallback 여부를 판단한다). 값이 NULL인 컬럼은 dict에 키 자체가 안
    들어간다 - 그 필드가 원래 없었던 것과 동일하게 취급하기 위함.

    conn을 넘기면 그 커넥션을 쓴다(batch_write_items와 동일한 패턴) - 서빙
    조회가 fast-fail 전용 커넥션(짧은 statement_timeout)을 쓰고 싶을 때
    공유 커넥션 대신 이걸로 지정한다.

    호출 하나(청크 여러 개 포함)에 걸린 시간을 로그로 남긴다 - RDS 자체
    CloudWatch 지표(ReadLatency 등)는 평균값에 디스크 I/O 지연이라, 서빙이
    실제 체감하는 쿼리 응답시간의 p50/p95/p99는 이 로그를 CloudWatch Logs
    Insights로 집계해서 봐야 한다(Grafana "데이터 신선도" 행 옆 참고).
    """
    if not keys:
        return {}

    _validate_identifier(table_name)
    conn = conn or _get_connection()
    key_columns = tuple(keys[0])
    if not key_columns or any(set(key) != set(key_columns) for key in keys):
        raise ValueError("모든 조회 키는 동일한 키 컬럼을 가져야 합니다")
    for name in key_columns:
        _validate_identifier(name)

    result: dict[tuple, dict] = {}

    if len(key_columns) == 1:
        query = sql.SQL("SELECT * FROM {table} WHERE {key} = ANY(%s)").format(
            table=sql.Identifier(table_name),
            key=sql.Identifier(key_columns[0]),
        )
    else:
        query = sql.SQL("SELECT * FROM {table} WHERE ({keys}) IN %s").format(
            table=sql.Identifier(table_name),
            keys=sql.SQL(", ").join(sql.Identifier(name) for name in key_columns),
        )

    start = time.perf_counter()
    for chunk in _chunk(keys, _BATCH_GET_CHUNK):
        key_values = tuple(tuple(key[name] for name in key_columns) for key in chunk)
        with conn.cursor() as cur:
            params = ([values[0] for values in key_values],) if len(key_columns) == 1 else (key_values,)
            cur.execute(query, params)
            column_names = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                row_dict = dict(zip(column_names, row))
                item = {}
                for key, value in row_dict.items():
                    if value is not None:
                        item[key] = _normalize_value(value)
                result[tuple(item[name] for name in key_columns)] = item
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(f"[rds_query_duration] table={table_name} ms={elapsed_ms:.1f}")

    return result


def get_item(table_name: str, key: dict) -> dict | None:
    """단건 조회. 없으면 None을 반환한다."""
    items = batch_get_items(table_name, [key])
    return items.get(tuple(key.values()))


def get_value(table_name: str, key: dict, value_column: str, default=0):
    """키 하나를 조회해 value_column 값을 돌려준다. 없으면 default다 —
    "무결점 응답" 원칙: 값이 없어도, 심지어 테이블 자체가 아직 안
    만들어졌어도(Gold 파이프라인이 한 번도 안 돈 경우 등) 절대 None/에러를
    반환하지 않는다. 지정한 값 컬럼이 없는 테이블에 잘못 호출하면 KeyError로
    바로 드러난다 - 그건 값이 없는 게 아니라 호출부
    버그이므로 조용히 감추지 않는다."""
    try:
        item = get_item(table_name, key)
    except psycopg2.errors.UndefinedTable:
        return default
    return item[value_column] if item is not None else default


def put_item(
    table_name: str,
    item: dict,
    key_columns: tuple[str, ...] = ("segment_id",),
) -> None:
    """항목 하나를 저장(upsert)한다."""
    batch_write_items(table_name, [item], key_columns=key_columns)


def batch_write_items(
    table_name: str,
    items: list[dict],
    key_columns: tuple[str, ...] = ("segment_id",),
    conn=None,
) -> None:
    """여러 항목을 한 번에 저장(upsert)한다.

    이 함수는 테이블 스키마를 미리 몰라도 된다 - 넘어온 items의 키 합집합이
    곧 이번에 쓸 컬럼 목록이다. 항목마다 필드가 달라도 없는 컬럼은 그
    행에서 NULL로 채운다. updated_date 같은 갱신 시각도 이제 DB가 자동으로
    채워주지 않으므로 호출부가 item dict에 직접 넣어야 한다.

    conn을 넘기면 그 커넥션을 쓴다(Spark 파티션별 전용 커넥션 등 공유
    커넥션을 쓰면 안 되는 호출부용) - 안 넘기면 공유 커넥션을 쓴다.
    """
    if not items:
        return

    _validate_identifier(table_name)
    conn = conn or _get_connection()

    if not key_columns:
        raise ValueError("기본키 컬럼이 하나 이상 필요합니다")
    for name in key_columns:
        _validate_identifier(name)

    parsed = []
    for item in items:
        key_values = tuple(item[name] for name in key_columns)
        extra = {k: v for k, v in item.items() if k not in key_columns}
        parsed.append((key_values, extra))

    extra_columns = sorted({key for _, extra in parsed for key in extra})
    for name in extra_columns:
        _validate_identifier(name)

    columns = list(key_columns) + extra_columns
    rows = [
        key_values
        + tuple(
            Json(extra[col]) if isinstance(extra.get(col), (dict, list)) else extra.get(col)
            for col in extra_columns
        )
        for key_values, extra in parsed
    ]

    set_parts = [
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
        for col in extra_columns
    ]

    query = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
    ).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in key_columns),
        set_clause=sql.SQL(", ").join(set_parts),
    )

    with conn.cursor() as cur:
        execute_values(cur, query.as_string(conn), rows)


def replace_table_snapshot(
    table_name: str,
    items: list[dict],
    key_columns: tuple[str, ...] = ("segment_id",),
    conn=None,
) -> int:
    """테이블 전체를 items로 완전히 교체한다(staging 테이블에 적재 후 원자적
    rename 스왑) — src/tlc/gold2.py의 Type3 스왑(write_type3_rolling_to_rds)과
    같은 원리를, 그보다 훨씬 작은 규모(수십만 행)의 전체 스냅샷 쓰기에 맞게
    축소한 버전이다(파티션 병렬 COPY 없이 단일 커넥션 execute_values로 충분).

    batch_write_items(upsert)와 달리 이번 items에 없는 기존 행은 스왑과
    함께 사라진다 — 그래서 "이번 호출 = 그 시점의 완전한 정답 집합"인
    호출부만 써야 한다(세그먼트가 대상에서 빠지면 이전 값이 자동으로
    삭제되길 원하는 길이/통행료 같은 전체 재계산 파이프라인). 대상
    테이블은 미리 존재해야 한다(직접 만들었거나 ensure_table로) —
    CREATE TABLE ... LIKE로 스키마/제약/인덱스를 그대로 복제하기 때문이다.

    items가 비어 있으면 스왑하지 않고 0을 반환한다 — 상류 버그(예: LION
    읽기가 빈 결과를 반환)로 빈 리스트가 들어온 경우까지 그대로 진행하면
    테이블 전체를 실수로 비워버리게 되므로, 이런 "명백히 잘못된 입력"은
    조용히 넘기지 않고 기존 테이블을 그대로 둔 채 호출부가 알아채게 한다.

    conn을 넘기면 그 커넥션을 쓴다 - 안 넘기면 공유 커넥션을 쓴다. 스왑
    자체는 이 함수 안에서 autocommit을 잠깐 끄고 하나의 트랜잭션으로
    묶는다(두 RENAME 사이에 테이블 이름이 존재하지 않는 순간이 없게).
    """
    _validate_identifier(table_name)
    if not key_columns:
        raise ValueError("기본키 컬럼이 하나 이상 필요합니다")
    for name in key_columns:
        _validate_identifier(name)

    if not items:
        logger.warning(
            f"[rds_snapshot_swap] table={table_name} items가 비어 있어 스왑을 건너뜁니다"
            " - 상류 버그로 빈 스냅샷이 들어온 게 아닌지 확인하세요"
        )
        return 0

    conn = conn or _get_connection()
    staging_table = f"{table_name}_staging_{uuid4().hex[:8]}"
    old_table = f"{table_name}_old_{uuid4().hex[:8]}"

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TABLE {staging} (LIKE {table} INCLUDING ALL)").format(
                    staging=sql.Identifier(staging_table),
                    table=sql.Identifier(table_name),
                )
            )

        extra_columns = sorted({key for item in items for key in item if key not in key_columns})
        for name in extra_columns:
            _validate_identifier(name)
        columns = list(key_columns) + extra_columns
        rows = [
            tuple(item[name] for name in key_columns)
            + tuple(
                Json(item[col]) if isinstance(item.get(col), (dict, list)) else item.get(col)
                for col in extra_columns
            )
            for item in items
        ]
        insert_query = sql.SQL("INSERT INTO {staging} ({cols}) VALUES %s").format(
            staging=sql.Identifier(staging_table),
            cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        with conn.cursor() as cur:
            execute_values(cur, insert_query.as_string(conn), rows)

        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("ALTER TABLE {table} RENAME TO {old}").format(
                        table=sql.Identifier(table_name),
                        old=sql.Identifier(old_table),
                    )
                )
                cur.execute(
                    sql.SQL("ALTER TABLE {staging} RENAME TO {table}").format(
                        staging=sql.Identifier(staging_table),
                        table=sql.Identifier(table_name),
                    )
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = True
    finally:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging_table))
            )
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(old_table)))

    logger.info(f"[rds_snapshot_swap] table={table_name} count={len(items)}")
    return len(items)


def cleanup_keys_not_in(
    table_name: str,
    valid_values: list[str],
    key_column: str = "segment_id",
    conn=None,
) -> dict:
    """table_name에서 key_column 값이 valid_values 집합에 없는 행을 전부
    삭제한다. LION처럼 "유효 대상 집합"이 가끔(분기 단위 등) 바뀌는
    source of truth가 있지만, 그 테이블 자체는 매번 전체를 다시 쓰지 않는
    경우(Type1의 30분 증분 upsert 등 — replace_table_snapshot을 못 쓰는
    경우)에 이 함수로 폐기된 행만 정리한다.

    NOT IN (...)에 파라미터 수십만 개를 직접 박으면 쿼리 플래너가
    죽으므로, valid_values를 임시 스테이징 테이블에 적재하고
    안티조인(NOT EXISTS)으로 삭제한다. 탐색(어떤 행이 stale인지)과
    삭제를 별도 SELECT/DELETE 두 문장으로 나누지 않는다 - 유효 행은
    안티조인 DELETE 한 문장으로도 어차피 읽히기만 하고 쓰기 락이 안
    걸리므로(NOT EXISTS 서브쿼리는 일반 읽기), 문장을 나눠도 스캔
    비용이 줄지 않으면서 테이블을 두 번 접근하게 되고 그 사이 경쟁
    구간(두 문장 사이 다른 트랜잭션이 끼어들 여지)만 새로 생긴다.
    RETURNING으로 탐색 결과(어떤 키가 지워졌는지)까지 같은 문장에서
    같이 얻는다.

    매 호출이 "지금 유효한 집합 vs RDS 실제 상태"를 직접 비교해서
    스스로 수렴하는 멱등(level-triggered) 연산이다 - 이전 실행이
    성공했는지 실패했는지에 의존하는 상태(마지막 성공 버전 추적 등)가
    전혀 없다. 그래서 실패해도 다음 실행이 처음부터 다시 정확한
    결과로 수렴하고, 별도 재시도/버전관리 로직이 필요 없다.

    (참고: 이 함수와 별개로, "정리 직후 구버전 LION을 참조하는 다른
    실행이 방금 지운 키를 다시 upsert하는" 경쟁은 여기서 안 막는다 -
    그걸 엄격히 막으려면 호출부(Airflow DAG)가 해당 쓰기 파이프라인과
    이 정리 태스크를 같은 1-slot pool로 직렬화해야 한다.)

    valid_values가 비어 있으면 삭제하지 않고 예외를 던진다 - 상류 버그로
    빈 유효집합이 들어온 경우까지 그대로 진행하면 테이블 전체가
    삭제되므로, 이런 입력은 "그 시점에 정말로 유효한 행이 하나도 없다"는
    뜻일 가능성보다 버그일 가능성이 훨씬 높다고 보고 조용히 넘기지 않는다.
    valid_values에 중복이 있어도 안전하다 - 스테이징 테이블 PK 위반을
    피하려고 미리 중복을 제거한다.

    반환값: {"valid_count", "stale_keys"(중복 제거된 key_column 값
    목록), "deleted_rows"(실제 삭제된 행 수 - key_column이 복합키
    일부라 stale_keys 하나당 여러 행일 수 있음)}. 로그/Slack 알림에
    필요한 지표를 이 호출 하나로 다 얻을 수 있다."""
    _validate_identifier(table_name)
    _validate_identifier(key_column)
    if not valid_values:
        raise ValueError(
            f"valid_values가 비어 있습니다(table={table_name}) - 상류 버그로 빈 "
            "유효집합이 들어온 게 아닌지 확인하세요. 정말로 테이블을 전부 "
            "비우려면 이 함수 대신 명시적으로 DELETE를 실행하세요."
        )
    valid_values = sorted(set(valid_values))

    conn = conn or _get_connection()
    staging_table = f"_valid_keys_{uuid4().hex[:8]}"

    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE TEMP TABLE {staging} ({col} TEXT PRIMARY KEY)").format(
                    staging=sql.Identifier(staging_table),
                    col=sql.Identifier(key_column),
                )
            )
            insert_query = sql.SQL("INSERT INTO {staging} VALUES %s").format(
                staging=sql.Identifier(staging_table)
            )
            execute_values(cur, insert_query.as_string(conn), [(v,) for v in valid_values])
            # 방금 채운 스테이징 테이블은 통계가 없어 플래너가 안티조인
            # 실행계획(해시/머지 안티조인 등)을 잘못 고를 수 있다 -
            # Type3 스왑(gold2.py)의 ANALYZE와 같은 이유.
            cur.execute(sql.SQL("ANALYZE {staging}").format(staging=sql.Identifier(staging_table)))

            cur.execute(
                sql.SQL(
                    "DELETE FROM {table} AS t WHERE NOT EXISTS "
                    "(SELECT 1 FROM {staging} AS v WHERE v.{col} = t.{col}) "
                    "RETURNING t.{col}"
                ).format(
                    table=sql.Identifier(table_name),
                    staging=sql.Identifier(staging_table),
                    col=sql.Identifier(key_column),
                )
            )
            deleted_rows = cur.fetchall()
    finally:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(staging_table))
            )

    # RETURNING은 삭제된 행마다 하나씩 나온다 - key_column이 기본키
    # 전체가 아니라 복합키 일부(예: Type1의 (segment_id, time))일 수
    # 있어서, 같은 key_column 값이 여러 행에 걸쳐 중복될 수 있다(세그먼트
    # 하나가 지워지면 그 세그먼트의 모든 time 슬롯이 한꺼번에 지워지는
    # 식). "어떤 키가 지워졌는지"와 "몇 행이 지워졌는지"를 둘 다
    # 돌려준다 - 전자는 알림 샘플용, 후자는 삭제 규모 지표용.
    stale_keys = sorted({row[0] for row in deleted_rows})
    result = {
        "valid_count": len(valid_values),
        "stale_keys": stale_keys,
        "deleted_rows": len(deleted_rows),
    }
    logger.info(
        f"[rds_stale_key_cleanup] table={table_name} valid_count={result['valid_count']} "
        f"stale_count={len(stale_keys)} deleted_rows={result['deleted_rows']}"
    )
    return result
