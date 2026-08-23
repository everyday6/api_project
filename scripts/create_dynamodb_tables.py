"""
DynamoDB 테이블 생성 스크립트 (idempotent)

배포 시 한 번 실행한다. 이미 테이블이 있으면 건너뛴다.

    python scripts/create_dynamodb_tables.py
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from src.common.config import (
    DYNAMODB_NAV_TABLE,
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    NAV_GOLD_TABLE,
)
from src.common.dynamodb import ensure_table
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="create_dynamodb_tables")

# 타입별로 완전히 분리된 테이블을 쓴다(src/common/config.py "세그먼트 지표
# API" 절 참고) — 여기 나열된 순서/목록이 지금 실제로 쓰이는 전체 테이블이다.
_ALWAYS_CREATE = (DYNAMODB_TABLE_TYPE1, DYNAMODB_TABLE_TYPE2, NAV_GOLD_TABLE)


def create_table_if_not_exists(table_name: str) -> None:
    try:
        ensure_table(table_name)
        logger.info(f"테이블 확인/생성 완료: {table_name}")
    except ClientError:
        logger.exception(f"테이블 생성 실패: {table_name}")
        raise


def main() -> None:
    for table_name in _ALWAYS_CREATE:
        create_table_if_not_exists(table_name)

    # DYNAMODB_NAV_TABLE(type3)은 env var라 기본값이 없다 — 배포 환경에
    # 아직 설정 전이면 여기서 건너뛰고 로그만 남긴다.
    if DYNAMODB_NAV_TABLE:
        create_table_if_not_exists(DYNAMODB_NAV_TABLE)
    else:
        logger.info("DYNAMODB_NAV_TABLE 환경변수가 없어 type3 테이블 생성은 건너뜀")


if __name__ == "__main__":
    main()
