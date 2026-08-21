"""
DynamoDB GLOBAL 기본값 시딩 스크립트

fallback 체인의 마지막 안전망(설계 문서 7절 3단계)이다. 파이프라인이 한
번도 성공적으로 안 돌았어도 이 값이 있어야 API가 "무조건 응답"할 수
있으므로, 파이프라인 코드가 아니라 배포 시점에 이 스크립트로 수동 시딩한다.

    python scripts/seed_dynamodb_defaults.py

기본값은 TODO(팀 검토 필요): 실측 데이터 없이 잡은 정성적 초안이다.
"""

from __future__ import annotations

from src.common.config import (
    DEFAULT_SORT_KEY,
    DYNAMODB_TABLE_TYPE1,
    DYNAMODB_TABLE_TYPE2,
    GLOBAL_PARTITION_KEY,
)
from src.common.dynamodb import put_item
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="seed_dynamodb_defaults")

# TODO(팀 검토 필요): NYC 평균 도로 세그먼트 기준 정성적 초안.
DEFAULT_TYPE1_SECONDS = 45
DEFAULT_TYPE2_LENGTH_FT = 300


def seed_defaults(
    type1_default: int = DEFAULT_TYPE1_SECONDS,
    type2_default: int = DEFAULT_TYPE2_LENGTH_FT,
) -> None:
    put_item(
        DYNAMODB_TABLE_TYPE1,
        {"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY, "value": type1_default},
    )
    logger.info(f"type1 GLOBAL#DEFAULT 시딩 완료: value={type1_default}")

    put_item(
        DYNAMODB_TABLE_TYPE2,
        {"segment_id": GLOBAL_PARTITION_KEY, "sk": DEFAULT_SORT_KEY, "value": type2_default},
    )
    logger.info(f"type2 GLOBAL#DEFAULT 시딩 완료: value={type2_default}")


def main() -> None:
    seed_defaults()


if __name__ == "__main__":
    main()
