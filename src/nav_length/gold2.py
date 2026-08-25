"""
Gold2 — type2(길이) 최종 산출물을 RDS 포맷으로 변환하고 upsert한다.

RDS는 세그먼트당 항목 1개(length_ft)만 저장한다 — 길이는 시간에
따라 변하지 않으므로 버킷을 반복 저장하지 않는다(설계 문서 6절).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame

from src.common.config import SERVING_TABLE_TYPE2_KEY_COLUMNS
from src.common.db import batch_write_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_length_gold2")


def to_serving_items(df: DataFrame, *, today: date | None = None) -> list[dict]:
    """(segment_id, length_ft) Spark DataFrame을 RDS 항목 리스트로 변환한다.

    결과가 작아(세그먼트당 1개, 최대 몇십만 건) 드라이버로 collect해도 안전하다.

    length_ft는 LION 원본을 그대로 반영한 정적 참조값이라 "수집일"이라는
    개념이 따로 없다 - updated_date(이 Gold2 실행일)만 채운다. 예전엔
    collected_date도 항상 같은 값으로 같이 채웠는데, 실행일 하나를 두
    컬럼에 중복 저장하는 것뿐이라 컬럼 자체를 없앴다(2026-08-25 스키마
    정리 - src/common/config.py의 SERVING_TABLE_TYPE2_COLUMNS 참고).
    today를 인자로 받는 건 테스트에서 고정된 날짜로 검증하기 위함이고,
    안 넘기면 실제 실행일(오늘)을 쓴다.
    """
    today = today or date.today()
    rows = df.select("segment_id", "length_ft").collect()

    return [
        {
            "segment_id": row["segment_id"],
            "value": round(row["length_ft"]),
            "updated_date": today.isoformat(),
        }
        for row in rows
    ]


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS(PostgreSQL)에 upsert하고 쓴 항목 수를 반환한다."""
    batch_write_items(table_name, items, key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS)
    logger.info(f"[nav_length_gold2] RDS upsert 완료: table={table_name} count={len(items)}")
    return len(items)
