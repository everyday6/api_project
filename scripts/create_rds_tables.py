"""
RDS(PostgreSQL) 서빙 테이블 생성 스크립트 (idempotent)

배포 시 한 번 실행한다. 이미 테이블이 있으면 건너뛴다.
DynamoDB 사용을 완전히 중단하면서 예전 create_dynamodb_tables.py는 삭제했다.

    python scripts/create_rds_tables.py
"""

from __future__ import annotations

import psycopg2

from src.common.config import (
    SERVING_TABLE_TYPE1,
    SERVING_TABLE_TYPE1_COLUMNS,
    SERVING_TABLE_TYPE1_KEY_COLUMNS,
    SERVING_TABLE_TYPE2,
    SERVING_TABLE_TYPE2_COLUMNS,
    SERVING_TABLE_TYPE2_KEY_COLUMNS,
    SERVING_TABLE_TYPE3,
    SERVING_TABLE_TYPE3_COLUMNS,
    SERVING_TABLE_TYPE3_KEY_COLUMNS,
    SERVING_TABLE_TYPE4,
    SERVING_TABLE_TYPE4_COLUMNS,
    SERVING_TABLE_TYPE4_KEY_COLUMNS,
)
from src.common.db import ensure_table
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="create_rds_tables")

# 타입별로 완전히 분리된 테이블을 쓴다(src/common/config.py "세그먼트 지표
# API" 절 참고) — 여기 나열된 순서/목록이 지금 실제로 쓰이는 전체 테이블이다.
# 테이블마다 스키마가 달라서(config.py의 *_COLUMNS) (테이블명, 컬럼정의)
# 쌍으로 관리한다.
_ALWAYS_CREATE = (
    (SERVING_TABLE_TYPE1, SERVING_TABLE_TYPE1_COLUMNS, SERVING_TABLE_TYPE1_KEY_COLUMNS),
    (SERVING_TABLE_TYPE2, SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS),
    (SERVING_TABLE_TYPE3, SERVING_TABLE_TYPE3_COLUMNS, SERVING_TABLE_TYPE3_KEY_COLUMNS),
    (SERVING_TABLE_TYPE4, SERVING_TABLE_TYPE4_COLUMNS, SERVING_TABLE_TYPE4_KEY_COLUMNS),
)


def create_table_if_not_exists(
    table_name: str,
    columns: dict,
    key_columns: tuple[str, ...],
) -> bool:
    """테이블을 만들고 성공 여부를 반환한다.

    권한 부족/커넥션 실패 등으로 실패해도 예외를 던지지 않는다 —
    이 스크립트는 airflow-init 안에서 DB 마이그레이션/관리자 계정 생성
    뒤에 실행되는데, 테이블 하나 못 만든다고 여기서 죽어버리면 Airflow
    전체(scheduler/worker/apiserver)가 못 뜨게 된다. 테이블 하나가 막힌
    것보다 Airflow 전체가 안 뜨는 게 훨씬 더 나쁘다 — 실패는 로그로
    남기고, 그 테이블을 실제로 쓰는 파이프라인이 나중에 쓰기 시도할 때
    실패로 드러나게 둔다."""
    try:
        ensure_table(table_name, columns, key_columns)
        logger.info(f"테이블 확인/생성 완료: {table_name}")
        return True
    except psycopg2.Error:
        logger.exception(f"테이블 생성 실패(건너뛰고 계속 진행): {table_name}")
        return False


def main() -> None:
    failed = []
    for table_name, columns, key_columns in _ALWAYS_CREATE:
        if not create_table_if_not_exists(table_name, columns, key_columns):
            failed.append(table_name)

    if failed:
        logger.warning(f"생성 실패한 테이블: {failed}")


if __name__ == "__main__":
    main()
