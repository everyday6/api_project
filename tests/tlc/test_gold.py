import pandas as pd

from src.tlc.gold import _expand_zone_to_segment_hour, _normalize_tlc_volume


def test_expand_zone_to_segment_hour_fills_missing_with_zero():
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
    })
    zone_hour_counts = pd.DataFrame({
        "zone_id": [1, 2],
        "hour": [8, 8],
        "dropoff_count": [100, 5],
    })

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)

    # 세그먼트 3개 x 24시간
    assert len(result) == 3 * 24
    assert set(result.columns) == {"segment_id", "hour", "dropoff_count_raw"}

    hour8 = result[result["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == 100
    assert hour8["B"] == 100  # 같은 zone(1)이면 zone 총합을 그대로 복사
    assert hour8["C"] == 5

    hour9 = result[result["hour"] == 9].set_index("segment_id")["dropoff_count_raw"]
    assert hour9["A"] == 0  # 트립이 없던 시간대는 0으로 채움


def test_expand_zone_to_segment_hour_every_segment_has_24_hours():
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    zone_hour_counts = pd.DataFrame({"zone_id": [], "hour": [], "dropoff_count": []})

    result = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)

    assert sorted(result["hour"].tolist()) == list(range(24))


def test_normalize_tlc_volume_percentile_rank():
    df = pd.DataFrame({
        "segment_id": ["A", "B", "C", "D", "E"],
        "hour": [0, 0, 0, 0, 0],
        "dropoff_count_raw": [0, 0, 5, 20, 100],
    })

    result = _normalize_tlc_volume(df)

    values = result.set_index("segment_id")["tlc_volume"]
    assert values["A"] == 0.3
    assert values["B"] == 0.3  # 동점(0)은 평균 등수를 받음
    assert values["C"] == 0.6
    assert values["D"] == 0.8
    assert values["E"] == 1.0


def test_normalize_tlc_volume_keeps_original_columns():
    df = pd.DataFrame({
        "segment_id": ["A", "B"],
        "hour": [0, 1],
        "dropoff_count_raw": [1, 2],
    })

    result = _normalize_tlc_volume(df)

    assert list(result.columns) == ["segment_id", "hour", "dropoff_count_raw", "tlc_volume"]
