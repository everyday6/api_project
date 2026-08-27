"""공용 테스트 픽스처.

RDS(PostgreSQL) 기반 통합 테스트가 여럿 있다. DynamoDB 시절엔 moto로 완전히
인메모리 목업이 가능했지만, PostgreSQL엔 그런 동급 라이브러리가 없어 실제
Postgres가 필요하다 - 로컬/CI에서 `docker compose up -d postgres`로 띄운
인스턴스에 그대로 붙는다(.env의 RDS_HOST 등 설정 재사용, docker-compose.yml
기본값은 로컬 postgres 서비스를 가리킨다).
"""

from __future__ import annotations

import pytest
from psycopg2 import sql

from src.common import db


def _postgres_reachable() -> bool:
    try:
        conn = db.new_connection()
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def require_postgres():
    """RDS(PostgreSQL)에 연결 못 하면 이 fixture를 쓰는 테스트 전체를
    스킵한다(에러로 실패시키지 않음) - `docker compose up -d postgres`를
    안 띄웠거나 .env에 RDS_* 설정이 없는 환경에서도 나머지 테스트 스위트는
    정상 실행되게 하기 위함."""
    if not _postgres_reachable():
        pytest.skip(
            "RDS(PostgreSQL)에 연결할 수 없습니다 - "
            "`docker compose up -d postgres` 실행 후 .env의 RDS_HOST/RDS_DB 등을 확인하세요."
        )


def reset_table(
    table_name: str,
    columns: dict[str, str],
    key_columns: tuple[str, ...] = ("segment_id",),
) -> None:
    """테스트용 테이블을 완전히 비운 상태로 새로 만든다.

    DynamoDB 시절 매 테스트가 @mock_aws로 받던 격리(테스트마다 완전히 빈
    테이블)를 재현한다 - 실제 Postgres는 테스트 사이에 데이터가 남으므로,
    테이블 자체를 지웠다 다시 만들어 이전 테스트의 잔여 데이터가 다음
    테스트에 영향을 주지 않게 한다.

    columns는 db.ensure_table()과 동일하게 그 테이블만의 컬럼 정의다 -
    테이블마다 스키마가 다르므로(src/common/config.py의 SERVING_TABLE_TYPE*_
    COLUMNS 참고) 호출부가 자기 타입에 맞는 걸 넘겨야 한다."""
    conn = db._get_connection()
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {table}").format(table=sql.Identifier(table_name)))
    db.ensure_table(table_name, columns, key_columns)
