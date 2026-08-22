"""
DynamoDB 테이블 생성 스크립트 (idempotent)

배포 시 한 번 실행한다. 이미 테이블이 있으면 건너뛴다.

    python scripts/create_dynamodb_tables.py
"""

from __future__ import annotations

from botocore.exceptions import ClientError

from src.common.config import DYNAMODB_TABLE_TYPE1, DYNAMODB_TABLE_TYPE2
from src.common.dynamodb import get_dynamodb_resource
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="create_dynamodb_tables")


def create_table_if_not_exists(table_name: str) -> None:
    resource = get_dynamodb_resource()

    existing = [t.name for t in resource.tables.all()]
    if table_name in existing:
        logger.info(f"이미 존재하는 테이블, 건너뜀: {table_name}")
        return

    try:
        table = resource.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "segment_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "segment_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        logger.info(f"테이블 생성 완료: {table_name}")
    except ClientError:
        logger.exception(f"테이블 생성 실패: {table_name}")
        raise


def main() -> None:
    create_table_if_not_exists(DYNAMODB_TABLE_TYPE1)
    create_table_if_not_exists(DYNAMODB_TABLE_TYPE2)


if __name__ == "__main__":
    main()
