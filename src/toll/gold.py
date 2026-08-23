"""
Gold — 통행료(혼잡/도로) 최종 값 계산 + DynamoDB 적재 + 서빙 조회

Silver2의 두 매핑(zone 안 segment, 시설 매칭 segment)에 요금표(Bronze)를
결합해서 최종 (segment_id, type) -> value를 만들고 DynamoDB에 쓴다.
혼잡/도로 통행료 둘 다 시간대에 따라 안 바뀌므로(택시 정액 요금 —
스펙의 Global Constraints 참고) sort key에 DATE/SLOT을 안 넣고
"TYPE#{n}" 고정 키만 쓴다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from src.common import dynamodb
from src.common.config import NAV_GOLD_TABLE
from src.common.logger import get_logger
from src.toll.silver2 import MAP_LION_CBD_PATH, MAP_LION_FACILITY_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_gold")

TYPE_CONGESTION = 4
TYPE_ROAD_TOLL = 5


def load_rate_table(path: Path = Path("data/bronze/toll/toll_rates.yaml")) -> dict:
    return yaml.safe_load(Path(path).read_text())


def build_gold_items(rate_table: dict, zone_map: pd.DataFrame, facility_map: pd.DataFrame) -> list[dict]:
    """혼잡통행료(zone 안 전 segment, 정액) + 도로통행료(시설 매칭 segment,
    요금표에 있는 시설만) 아이템 목록을 만든다. 요금표에 없는 facility_key는
    (예: 시설 목록엔 있는데 아직 요금이 안 채워진 경우) 조용히 건너뛴다 —
    값을 지어내지 않는다."""

    items = []

    taxi_flat_rate = rate_table["congestion"]["taxi_flat_rate"]
    for segment_id in zone_map["segment_id"]:
        items.append({
            "segment_id": segment_id,
            "sk": f"TYPE#{TYPE_CONGESTION}",
            "value": taxi_flat_rate,
        })

    road_rates = rate_table["road"]
    for _, row in facility_map.iterrows():
        facility_key = row["facility_key"]
        if facility_key not in road_rates:
            logger.warning(f"[toll_gold] 요금표에 없는 시설 건너뜀: {facility_key}")
            continue
        items.append({
            "segment_id": row["segment_id"],
            "sk": f"TYPE#{TYPE_ROAD_TOLL}",
            "value": road_rates[facility_key]["passenger"],
        })

    return _dedupe_items(items)


def _dedupe_items(items: list[dict]) -> list[dict]:
    """(segment_id, sk) 기준으로 중복을 제거한다(첫 값 유지). DynamoDB
    batch_write_item은 같은 배치 안에 동일 키가 두 번 있으면 통째로
    에러를 낸다 — 실제로 LION 원본에 중복 segment_id 행이 있어서 겪었다
    (load_lion_segments에서 1차로 제거하지만, 여기서도 한 번 더 방어)."""

    seen: set[tuple] = set()
    deduped = []
    for item in items:
        key = (item["segment_id"], item["sk"])
        if key in seen:
            logger.warning(f"[toll_gold] 중복 키 건너뜀: {key}")
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def write_gold_items(items: list[dict]) -> None:
    # ensure_table은 원래 로컬/테스트 편의용이라 운영 경로에서는 안 쓰는 게
    # 원칙이지만, 이 파이프라인은 처음부터 그렇게 짜여 있었고 여기서 빼면
    # 배포 순서(scripts/create_dynamodb_tables.py를 먼저 돌려야 함)에
    # 새로 의존하게 되어 그대로 유지한다. idempotent라 반복 호출해도 안전.
    dynamodb.ensure_table(NAV_GOLD_TABLE)
    dynamodb.batch_write_items(NAV_GOLD_TABLE, items)
    logger.info(f"[toll_gold] DynamoDB에 {len(items)}개 아이템 적재 완료")


def build_and_write(
    rate_table_path: Path = Path("data/bronze/toll/toll_rates.yaml"),
    lion_cbd_map_path: Path = MAP_LION_CBD_PATH,
    lion_facility_map_path: Path = MAP_LION_FACILITY_PATH,
) -> int:
    rate_table = load_rate_table(rate_table_path)
    zone_map = pd.read_parquet(str(lion_cbd_map_path))
    facility_map = pd.read_parquet(str(lion_facility_map_path))

    items = build_gold_items(rate_table, zone_map, facility_map)
    write_gold_items(items)
    return len(items)


def get_toll_value(segment_id: str, toll_type: int) -> float:
    """서빙 조회 함수. 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음)."""

    return dynamodb.get_value(NAV_GOLD_TABLE, segment_id, f"TYPE#{toll_type}", default=0)


if __name__ == "__main__":
    build_and_write()
