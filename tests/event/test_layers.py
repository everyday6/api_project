import pandas as pd

from src.event.gold1 import filter_for_traffic_score, validate
from src.silver2.event_lion import map_event_to_lion, prepare_lion


def _mapped_event(
    event_id: str,
    borough: str,
    end_ts: str,
    closure_type: str,
    segment_id: str,
) -> dict:
    return {
        "event_id": event_id,
        "event_borough": borough,
        "start_ts": pd.Timestamp("2026-08-20 10:00:00"),
        "end_ts": pd.Timestamp(end_ts),
        "closure_type": closure_type,
        "on_street": "BROADWAY",
        "from_street": "WEST 31 STREET",
        "to_street": "WEST 32 STREET",
        "segment_id": segment_id,
        "is_routable": True,
        "mapping_status": "matched",
        "unmatched_reason": None,
    }


def test_gold1_filters_event_lion_silver2_rows():
    silver2 = pd.DataFrame([
        _mapped_event("keep", "Manhattan", "2026-08-21", "Full Street Closure", "s1"),
        _mapped_event("keep", "Manhattan", "2026-08-21", "Full Street Closure", "s2"),
        _mapped_event("outside", "Queens", "2026-08-21", "Full Street Closure", "s3"),
        _mapped_event("past", "Manhattan", "2026-08-19", "Full Street Closure", "s4"),
        _mapped_event("sidewalk", "Manhattan", "2026-08-21", "Full Sidewalk Closure", "s5"),
    ])

    result = filter_for_traffic_score(silver2, "2026-08-20")

    assert result["event_id"].tolist() == ["keep", "keep"]
    assert result["segment_id"].tolist() == ["s1", "s2"]
    validate(result)


def test_event_lion_silver2_keeps_all_boroughs():
    lion = pd.DataFrame([
        {
            "segment_id": "manhattan",
            "street_name": " broadway ",
            "node_from": "1",
            "node_to": "2",
            "borough_code": "1",
            "length_ft": "100",
            "is_routable": True,
        },
        {
            "segment_id": "queens",
            "street_name": " queens boulevard ",
            "node_from": "3",
            "node_to": "4",
            "borough_code": "4",
            "length_ft": "200",
            "is_routable": True,
        },
    ])

    result = prepare_lion(lion)

    assert result["segment_id"].tolist() == ["manhattan", "queens"]
    assert result["street_name"].tolist() == ["BROADWAY", "QUEENS BOULEVARD"]


def test_event_lion_maps_within_each_event_borough():
    lion_rows = []
    for prefix, borough_code in [("m", "1"), ("q", "4")]:
        lion_rows.extend([
            {
                "segment_id": f"{prefix}-main",
                "street_name": "BROADWAY",
                "node_from": f"{prefix}-a",
                "node_to": f"{prefix}-b",
                "borough_code": borough_code,
                "length_ft": 100,
                "is_routable": True,
            },
            {
                "segment_id": f"{prefix}-from",
                "street_name": "1 AVENUE",
                "node_from": f"{prefix}-a",
                "node_to": f"{prefix}-x",
                "borough_code": borough_code,
                "length_ft": 50,
                "is_routable": True,
            },
            {
                "segment_id": f"{prefix}-to",
                "street_name": "2 AVENUE",
                "node_from": f"{prefix}-b",
                "node_to": f"{prefix}-y",
                "borough_code": borough_code,
                "length_ft": 50,
                "is_routable": True,
            },
        ])

    events = pd.DataFrame([
        {
            "event_id": "manhattan-event",
            "event_borough": "Manhattan",
            "start_ts": pd.Timestamp("2026-08-20 10:00:00"),
            "end_ts": pd.Timestamp("2026-08-20 12:00:00"),
            "closure_type": "Full Street Closure",
            "on_street": "BROADWAY",
            "from_street": "1 AVENUE",
            "to_street": "2 AVENUE",
        },
        {
            "event_id": "queens-event",
            "event_borough": "Queens",
            "start_ts": pd.Timestamp("2026-08-20 10:00:00"),
            "end_ts": pd.Timestamp("2026-08-20 12:00:00"),
            "closure_type": "Full Street Closure",
            "on_street": "BROADWAY",
            "from_street": "1 AVENUE",
            "to_street": "2 AVENUE",
        },
    ])

    result = map_event_to_lion(events, pd.DataFrame(lion_rows))

    mapped = result.set_index("event_id")["segment_id"].to_dict()
    assert mapped == {
        "manhattan-event": "m-main",
        "queens-event": "q-main",
    }
