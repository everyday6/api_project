"""
DynamoDB 테이블 생성 스크립트 (idempotent)

배포 시 한 번 실행한다. 이미 테이블이 있으면 건너뛴다.

    python scripts/create_dynamodb_tables.py
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from src.common.config import (
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    DYNAMODB_TABLE_TYPE3,
    DYNAMODB_TABLE_TYPE4,
)
from src.common.dynamodb import ensure_table
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="create_dynamodb_tables")

# 타입별로 완전히 분리된 테이블을 쓴다(src/common/config.py "세그먼트 지표
# API" 절 참고) — 여기 나열된 순서/목록이 지금 실제로 쓰이는 전체 테이블이다.
# 네 타입 전부 이름/기본값 패턴이 통일되어 있어 특별 취급 없이 그대로 나열한다.
_ALWAYS_CREATE = (
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    DYNAMODB_TABLE_TYPE3,
    DYNAMODB_TABLE_TYPE4,
)


def create_table_if_not_exists(table_name: str) -> bool:
    """테이블을 만들고 성공 여부를 반환한다.

    권한 부족(AccessDenied) 등으로 실패해도 예외를 던지지 않는다 —
    이 스크립트는 airflow-init 안에서 DB 마이그레이션/관리자 계정 생성
    뒤에 실행되는데, 테이블 하나 못 만든다고 여기서 죽어버리면 Airflow
    전체(scheduler/worker/apiserver)가 못 뜨게 된다. 테이블 하나가 막힌
    것보다 Airflow 전체가 안 뜨는 게 훨씬 더 나쁘다 — 실패는 로그로
    남기고, 그 테이블을 실제로 쓰는 파이프라인이 나중에 쓰기 시도할 때
    실패로 드러나게 둔다."""
    try:
        ensure_table(table_name)
        logger.info(f"테이블 확인/생성 완료: {table_name}")
        return True
    except ClientError:
        logger.exception(f"테이블 생성 실패(건너뛰고 계속 진행): {table_name}")
        return False


def main() -> None:
    failed = []
    for table_name in _ALWAYS_CREATE:
        if not create_table_if_not_exists(table_name):
            failed.append(table_name)

    if failed:
        logger.warning(
            f"생성 실패한 테이블: {failed} — IAM 권한을 확인하세요. "
            "airflow-init 자체는 계속 진행합니다."
        )


if __name__ == "__main__":
    main()
