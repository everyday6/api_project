"""
Gold — 통행료(혼잡+도로) 최종 값 계산 + DynamoDB 적재 + 서빙 조회

Silver2의 두 매핑(zone 안 segment, 시설 매칭 segment)에 요금표(Bronze)를
결합해서 최종 (segment_id, type) -> value를 만들고 DynamoDB에 쓴다.
혼잡통행료와 도로통행료는 원래 type=4/5로 나눠서 저장했는데, 한 segment가
CBD zone 안이면서 동시에 다리/터널 시설이기도 한 경우(zone 진입 지점의
다리 segment 등) 클라이언트가 매번 두 타입을 다 조회해서 더해야 했다.
"택시가 이 segment를 지나는 데 드는 통행료 총액"이 실제로 필요한 값이므로,
여기서 미리 합산해 type=4(TYPE_TOLL) 하나로 합쳤다 — nav-gold 전체
설계(시간=1/길이=2/수요=3/통행료=4)의 4타입 구성과도 맞다.

통행료는 시간대에 따라 안 바뀌므로(택시 정액 요금 — 스펙의 Global
Constraints 참고) sort key에 DATE/SLOT을 안 넣고 "TYPE#{n}" 고정 키만
쓴다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from src.common import dynamodb
from src.common.config import DYNAMODB_TABLE_TYPE4
from src.common.logger import get_logger
from src.toll.bronze import BRONZE_ROOT
from src.toll.serving import TYPE_TOLL, get_toll_value  # noqa: F401 (하위 호환 재수출)
from src.toll.silver2 import MAP_LION_CBD_PATH, MAP_LION_FACILITY_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_gold")


def load_rate_table(path: Path = BRONZE_ROOT / "toll_rates.yaml") -> dict:
    return yaml.safe_load(Path(path).read_text())


def _congestion_values(rate_table: dict, zone_map: pd.DataFrame) -> dict[str, float]:
    """CBD zone 안 segment마다 택시 정액 혼잡통행료를 매긴다."""

    taxi_flat_rate = rate_table["congestion"]["taxi_flat_rate"]
    return {segment_id: taxi_flat_rate for segment_id in zone_map["segment_id"]}


def _road_toll_values(rate_table: dict, facility_map: pd.DataFrame) -> dict[str, float]:
    """시설 매칭 segment마다 도로통행료를 매긴다. 요금표에 없는 facility_key는
    (예: 시설 목록엔 있는데 아직 요금이 안 채워진 경우) 조용히 건너뛴다 —
    값을 지어내지 않는다. 한 segment가 두 시설에 매칭되는 경우(street 이름
    패턴이 겹치는 등)는 실제로는 있을 수 없는 상황이라, 첫 매칭만 남기고
    나머지는 경고만 남긴다."""

    road_rates = rate_table["road"]
    values: dict[str, float] = {}

    for _, row in facility_map.iterrows():
        segment_id, facility_key = row["segment_id"], row["facility_key"]

        if facility_key not in road_rates:
            logger.warning(f"[toll_gold] 요금표에 없는 시설 건너뜀: {facility_key}")
            continue
        if segment_id in values:
            logger.warning(f"[toll_gold] 이미 다른 시설과 매칭된 segment, 첫 매칭 유지: {segment_id}")
            continue

        values[segment_id] = road_rates[facility_key]["passenger"]

    return values


def build_gold_items(rate_table: dict, zone_map: pd.DataFrame, facility_map: pd.DataFrame) -> list[dict]:
    """혼잡통행료 + 도로통행료를 segment_id별로 합산해 type=4 아이템 하나로
    만든다. zone에만 속하면 혼잡통행료만, 시설에만 매칭되면 도로통행료만,
    둘 다면 합산값이 된다 — 서로 독립된 조건이라 겹칠 수 있다(예: CBD 진입
    지점의 다리 segment)."""

    congestion_values = _congestion_values(rate_table, zone_map)
    road_toll_values = _road_toll_values(rate_table, facility_map)

    segment_ids = set(congestion_values) | set(road_toll_values)

    return [
        {
            "segment_id": segment_id,
            "sk": f"TYPE#{TYPE_TOLL}",
            "value": congestion_values.get(segment_id, 0) + road_toll_values.get(segment_id, 0),
        }
        for segment_id in segment_ids
    ]


def write_gold_items(items: list[dict]) -> None:
    # ensure_table은 원래 로컬/테스트 편의용이라 운영 경로에서는 안 쓰는 게
    # 원칙이지만, 이 파이프라인은 처음부터 그렇게 짜여 있었고 여기서 빼면
    # 배포 순서(scripts/create_dynamodb_tables.py를 먼저 돌려야 함)에
    # 새로 의존하게 되어 그대로 유지한다. idempotent라 반복 호출해도 안전.
    dynamodb.ensure_table(DYNAMODB_TABLE_TYPE4)
    dynamodb.batch_write_items(DYNAMODB_TABLE_TYPE4, items)
    logger.info(f"[toll_gold] DynamoDB에 {len(items)}개 아이템 적재 완료")


def build_and_write(
    rate_table_path: Path = BRONZE_ROOT / "toll_rates.yaml",
    lion_cbd_map_path: Path = MAP_LION_CBD_PATH,
    lion_facility_map_path: Path = MAP_LION_FACILITY_PATH,
) -> int:
    rate_table = load_rate_table(rate_table_path)
    zone_map = pd.read_parquet(str(lion_cbd_map_path))
    facility_map = pd.read_parquet(str(lion_facility_map_path))

    items = build_gold_items(rate_table, zone_map, facility_map)
    write_gold_items(items)
    return len(items)


if __name__ == "__main__":
    build_and_write()
