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

배치 조회/쓰기 시 예외를 삼키지 않고 그대로 던진다 — 호출부(서빙 API)가
그 예외를 잡아서 fallback으로 넘어간다. 유일한 예외는 get_value()의
"테이블 자체가 아직 없음" 케이스뿐이다.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal

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
