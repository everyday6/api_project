import pandas as pd
import yaml

from src.toll.gold import (
    TYPE_CONGESTION,
    TYPE_ROAD_TOLL,
    build_gold_items,
    get_toll_value,
    load_rate_table,
    write_gold_items,
)


def _write_rate_table(tmp_path):
    path = tmp_path / "toll_rates.yaml"
    path.write_text(yaml.dump({
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }))
    return path


def test_load_rate_table_reads_yaml(tmp_path):
    path = _write_rate_table(tmp_path)

    rates = load_rate_table(path)

    assert rates["congestion"]["taxi_flat_rate"] == 0.75
    assert rates["road"]["lincoln_tunnel"]["passenger"] == 17.00


def test_build_gold_items_creates_congestion_items_for_zone_segments(tmp_path):
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame({"segment_id": ["Z1", "Z2"]})
    facility_map = pd.DataFrame(columns=["segment_id", "facility_key"])

    items = build_gold_items(rate_table, zone_map, facility_map)

    congestion_items = [i for i in items if i["sk"] == f"TYPE#{TYPE_CONGESTION}"]
    assert {i["segment_id"] for i in congestion_items} == {"Z1", "Z2"}
    assert all(i["value"] == 0.75 for i in congestion_items)


def test_build_gold_items_creates_road_toll_items_with_passenger_fallback(tmp_path):
    rate_table = {
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }
    zone_map = pd.DataFrame(columns=["segment_id"])
    facility_map = pd.DataFrame({"segment_id": ["S1", "S2"], "facility_key": ["lincoln_tunnel", "lincoln_tunnel"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    road_items = [i for i in items if i["sk"] == f"TYPE#{TYPE_ROAD_TOLL}"]
    assert {i["segment_id"] for i in road_items} == {"S1", "S2"}
    assert all(i["value"] == 17.00 for i in road_items)


def test_build_gold_items_skips_facility_without_rate():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame(columns=["segment_id"])
    # rate_table에 없는 시설 -> 값을 못 만드니 결과에서 빠져야 한다.
    facility_map = pd.DataFrame({"segment_id": ["S1"], "facility_key": ["unknown_facility"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert items == []


def test_build_gold_items_dedupes_same_segment_and_type():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    # 같은 segment_id가 zone_map에 두 번 들어와도(예: 상류 데이터 중복)
    # 최종 아이템은 (segment_id, sk) 기준으로 하나만 남아야 한다 —
    # DynamoDB batch_write_item이 같은 배치 안 중복 키에 에러를 내기 때문.
    zone_map = pd.DataFrame({"segment_id": ["Z1", "Z1"]})
    facility_map = pd.DataFrame(columns=["segment_id", "facility_key"])

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert len(items) == 1
    assert items[0]["segment_id"] == "Z1"


def test_get_toll_value_returns_zero_when_not_found():
    result = get_toll_value("NO_SUCH_SEGMENT", 5)

    assert result == 0


def test_get_toll_value_returns_written_value():
    write_gold_items([{"segment_id": "S99", "sk": "TYPE#5", "value": 12.34}])

    result = get_toll_value("S99", 5)

    assert result == 12.34
