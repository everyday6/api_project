"""
RDS GLOBAL 기본값 시딩 스크립트

fallback 체인의 마지막 안전망(설계 문서 7절 3단계)이다. 파이프라인이 한
번도 성공적으로 안 돌았어도 이 값이 있어야 API가 "무조건 응답"할 수
있으므로, 파이프라인 코드가 아니라 배포 시점에 이 스크립트로 수동 시딩한다.
DynamoDB 사용을 완전히 중단하면서 예전 seed_dynamodb_defaults.py는 삭제했다.

    python scripts/seed_rds_defaults.py

type1(시간)은 여기서 시딩할 GLOBAL 기본값이 없다 - src/serving/nav_lookup.py의
fallback 체인이 Fresh Exact -> Historical AVG -> 코드 상수(_HARDCODED_DEFAULTS)
순서라, RDS에 아무 값도 없어도 코드 상수로 바로 응답한다. type2(길이)만
"진짜 있을 법한 값"으로 미리 심어두는 GLOBAL 행이 필요하다(코드 상수보다
실측에 가까운 값을 주고 싶어서).

기본값은 TODO(팀 검토 필요): 실측 데이터 없이 잡은 정성적 초안이다.
"""

from __future__ import annotations

from datetime import date

from src.common.config import (
    GLOBAL_PARTITION_KEY,
    SERVING_TABLE_TYPE2,
    SERVING_TABLE_TYPE2_COLUMNS,
    SERVING_TABLE_TYPE2_KEY_COLUMNS,
)
from src.common.db import ensure_table, put_item
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="seed_rds_defaults")

# TODO(팀 검토 필요): NYC 평균 도로 세그먼트 기준 정성적 초안.
DEFAULT_TYPE2_LENGTH_FT = 300


def seed_defaults(type2_default: int = DEFAULT_TYPE2_LENGTH_FT) -> None:
    # create_rds_tables.py가 먼저 돌았다는 전제지만, 배포 순서가 꼬여도
    # 이 스크립트 혼자 안전하게 돌 수 있게 idempotent하게 한 번 더 보장한다.
    ensure_table(
        SERVING_TABLE_TYPE2,
        SERVING_TABLE_TYPE2_COLUMNS,
        SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )

    today = date.today().isoformat()
    put_item(
        SERVING_TABLE_TYPE2,
        {
            "segment_id": GLOBAL_PARTITION_KEY,
            "value": type2_default,
            "collected_date": today,
            "updated_date": today,
        },
        key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )
    logger.info(f"type2 GLOBAL 기본값 시딩 완료: value={type2_default}")


def main() -> None:
    seed_defaults()


if __name__ == "__main__":
    main()
