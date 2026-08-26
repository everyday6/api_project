from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from src.toll import gold
from src.toll.gold import (
    build_gold_items,
    get_toll_value,
    load_rate_table,
    write_gold_items,
)

# 이 파일 대부분은 순수 로직(요금 계산) 테스트라 RDS가 없어도 도는데, 맨
# 아래 두 테스트만 실제 RDS 왕복이라 개별로 @requires_postgres를 붙인다.
requires_postgres = pytest.mark.usefixtures("require_postgres")


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


def test_build_gold_items_creates_congestion_only_items_for_zone_segments():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame({"segment_id": ["Z1", "Z2"]})
    facility_map = pd.DataFrame(columns=["segment_id", "facility_key"])

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert {i["segment_id"]: i["value"] for i in items} == {"Z1": 0.75, "Z2": 0.75}


def test_build_gold_items_creates_road_toll_only_items():
    rate_table = {
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }
    zone_map = pd.DataFrame(columns=["segment_id"])
    facility_map = pd.DataFrame({"segment_id": ["S1", "S2"], "facility_key": ["lincoln_tunnel", "lincoln_tunnel"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert {i["segment_id"]: i["value"] for i in items} == {"S1": 17.00, "S2": 17.00}


def test_build_gold_items_sums_congestion_and_road_toll_for_same_segment():
    # CBD zone 진입 지점의 다리 segment처럼 두 조건을 동시에 만족하는 경우 -
    # 실제로 택시가 그 segment를 지나는 데 드는 통행료 총액은 합산값이다.
    rate_table = {
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {"lincoln_tunnel": {"passenger": 17.00}},
    }
    zone_map = pd.DataFrame({"segment_id": ["B1"]})
    facility_map = pd.DataFrame({"segment_id": ["B1"], "facility_key": ["lincoln_tunnel"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert len(items) == 1
    assert items[0]["segment_id"] == "B1"
    assert items[0]["value"] == 17.75


def test_build_gold_items_skips_facility_without_rate():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    zone_map = pd.DataFrame(columns=["segment_id"])
    # rate_table에 없는 시설 -> 값을 못 만드니 결과에서 빠져야 한다.
    facility_map = pd.DataFrame({"segment_id": ["S1"], "facility_key": ["unknown_facility"]})

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert items == []


def test_build_gold_items_dedupes_same_segment_appearing_twice_in_zone_map():
    rate_table = {"congestion": {"taxi_flat_rate": 0.75}, "road": {}}
    # 같은 segment_id가 zone_map에 두 번 들어와도(예: 상류 데이터 중복)
    # 최종 아이템은 segment_id 기준으로 하나만 남아야 한다 —
    # RDS 배치 upsert가 같은 배치 안 중복 PK에 에러를 내기 때문.
    zone_map = pd.DataFrame({"segment_id": ["Z1", "Z1"]})
    facility_map = pd.DataFrame(columns=["segment_id", "facility_key"])

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert len(items) == 1
    assert items[0]["segment_id"] == "Z1"


def test_build_gold_items_keeps_first_match_when_segment_matches_two_facilities():
    # 한 segment가 서로 다른 두 시설에 매칭되는 건 실제로는 있을 수 없는
    # 상황(street 이름 패턴이 겹치는 등 데이터 이상)이지만, 방어적으로
    # 첫 매칭만 남긴다 - 합산하면 값을 지어내는 셈이 된다.
    rate_table = {
        "congestion": {"taxi_flat_rate": 0.75},
        "road": {
            "lincoln_tunnel": {"passenger": 17.00},
            "holland_tunnel": {"passenger": 16.79},
        },
    }
    zone_map = pd.DataFrame(columns=["segment_id"])
    facility_map = pd.DataFrame({
        "segment_id": ["S1", "S1"],
        "facility_key": ["lincoln_tunnel", "holland_tunnel"],
    })

    items = build_gold_items(rate_table, zone_map, facility_map)

    assert len(items) == 1
    assert items[0]["value"] == 17.00


def test_write_gold_items_exports_snapshot_after_rds_write():
    items = [{"segment_id": "S1", "value": 2.75}, {"segment_id": "S2", "value": 17.00}]

    with patch.object(gold.db, "ensure_table"), \
         patch.object(gold.db, "replace_table_snapshot") as mock_write, \
         patch.object(gold.gold_snapshot, "write_snapshot") as mock_snapshot:
        write_gold_items(items)

    mock_write.assert_called_once()
    mock_snapshot.assert_called_once_with("type4", {"S1": 2.75, "S2": 17.00})


def test_write_gold_items_survives_snapshot_export_failure():
    # 스냅샷 갱신이 실패해도 RDS 쓰기 자체는 이미 끝났으므로 예외를
    # 전파하면 안 된다.
    items = [{"segment_id": "S1", "value": 2.75}]

    with patch.object(gold.db, "ensure_table"), \
         patch.object(gold.db, "replace_table_snapshot"), \
         patch.object(gold.gold_snapshot, "write_snapshot", side_effect=RuntimeError("S3 down")):
        write_gold_items(items)  # 예외 없이 정상 종료돼야 한다.


@requires_postgres
def test_get_toll_value_returns_zero_when_not_found():
    result = get_toll_value("NO_SUCH_SEGMENT")

    assert result == 0


@requires_postgres
def test_get_toll_value_returns_written_value():
    write_gold_items([{"segment_id": "S99", "value": 12.34}])

    result = get_toll_value("S99")

    assert result == 12.34
