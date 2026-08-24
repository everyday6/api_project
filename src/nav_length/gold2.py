"""
Gold2 — type2(길이) 최종 산출물을 RDS 포맷으로 변환하고 upsert한다.

RDS는 세그먼트당 행 1개(시간 무관 정적값)만 저장한다 — 길이는 시간에
따라 변하지 않으므로 버킷을 반복 저장하지 않는다(설계 문서 6절).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame

from src.common import rds
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_length_gold2")


def to_type2_items(df: DataFrame) -> list[dict]:
    """(segment_id, length_ft) Spark DataFrame을 RDS 행 리스트로 변환한다.

    결과가 작아(세그먼트당 1개, 최대 몇십만 건) 드라이버로 collect해도 안전하다.
    collected_date/updated_date는 이 LION 스냅샷을 처리한 오늘 날짜로 채운다 -
    길이 자체는 LION 버전마다 갱신되는 정적값이라 판독 시각 같은 개념이 따로
    없다."""
    rows = df.select("segment_id", "length_ft").collect()
    today = date.today().isoformat()

    return [
        {
            "segment_id": row["segment_id"],
            "value": round(row["length_ft"]),
            "collected_date": today,
            "updated_date": today,
        }
        for row in rows
    ]


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS에 upsert하고 쓴 항목 수를 반환한다."""
    rds.ensure_static_table(table_name)
    count = rds.upsert_static_items(items, table_name)
    logger.info(f"[nav_length_gold2] RDS upsert 완료: table={table_name} count={count}")
    return count
