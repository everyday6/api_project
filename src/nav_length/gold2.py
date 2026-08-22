"""
Gold2 — type2(길이) 최종 산출물을 DynamoDB 포맷으로 변환하고 upsert한다.

DynamoDB는 세그먼트당 항목 1개(sk="LENGTH")만 저장한다 — 길이는 시간에
따라 변하지 않으므로 버킷을 반복 저장하지 않는다(설계 문서 6절).
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.common.config import LENGTH_SORT_KEY
from src.common.dynamodb import batch_write_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_length_gold2")


def to_dynamodb_items(df: DataFrame) -> list[dict]:
    """(segment_id, length_ft) Spark DataFrame을 DynamoDB 항목 리스트로 변환한다.

    결과가 작아(세그먼트당 1개, 최대 몇십만 건) 드라이버로 collect해도 안전하다.
    """
    rows = df.select("segment_id", "length_ft").collect()

    return [
        {"segment_id": row["segment_id"], "sk": LENGTH_SORT_KEY, "value": round(row["length_ft"])}
        for row in rows
    ]


def write_to_dynamodb(items: list[dict], table_name: str) -> int:
    """DynamoDB에 upsert하고 쓴 항목 수를 반환한다."""
    batch_write_items(table_name, items)
    logger.info(f"[nav_length_gold2] DynamoDB upsert 완료: table={table_name} count={len(items)}")
    return len(items)
