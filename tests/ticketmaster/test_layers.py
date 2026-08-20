from datetime import date

import pandas as pd
from src.ticketmaster.gold1 import filter_for_traffic_score
from src.ticketmaster.silver1 import normalize_venue, transform


def test_silver1_normalizes_venue_name():
    assert normalize_venue("  Broadway Theater-New York ") == "BROADWAY THEATRE"
    assert normalize_venue("Jacobs Theatre-NY") == "BERNARD B JACOBS THEATRE"
    assert normalize_venue(None) is None


def test_silver1_transform_adds_normalized_name():
    raw = pd.DataFrame([{
        "id": "event-1",
        "_embedded_venues": (
            '[{"name":"Broadway Theater-New York",'
            '"location":{"latitude":"40.76","longitude":"-73.98"}}]'
        ),
        "dates_start_localDate": "2026-09-01",
        "dates_start_localTime": "19:00:00",
        "dates_end_localDate": None,
        "dates_end_localTime": None,
    }])

    result = transform(raw)

    assert result.loc[0, "venue_name_norm"] == "BROADWAY THEATRE"


def test_gold1_filters_region_date_and_excluded_venue():
    rows = []
    for event_id, event_date, venue_name_norm, lat in [
        ("keep", date(2026, 9, 1), "MADISON SQUARE GARDEN", 40.75),
        ("excluded", date(2026, 9, 1), "BANKSY MUSEUM", 40.75),
        ("outside", date(2026, 9, 1), "MADISON SQUARE GARDEN", 40.95),
        ("past", date(2026, 7, 1), "MADISON SQUARE GARDEN", 40.75),
        ("unmapped", date(2026, 9, 1), "MADISON SQUARE GARDEN", 40.75),
    ]:
        is_unmapped = event_id == "unmapped"
        rows.append({
            "event_id": event_id,
            "event_date": event_date,
            "start_ts": pd.Timestamp(f"{event_date} 19:00:00"),
            "end_ts": pd.NaT,
            "venue_name": venue_name_norm.title(),
            "venue_name_norm": venue_name_norm,
            "lat": lat,
            "lon": -73.98,
            "segment_id": pd.NA if is_unmapped else f"segment-{event_id}",
            "distance_ft": 3500.0 if is_unmapped else 10.0,
            "mapping_method": "unmapped_too_far" if is_unmapped else "buffer",
        })

    result = filter_for_traffic_score(pd.DataFrame(rows), "2026-08-20")

    assert result["event_id"].tolist() == ["keep"]
    assert "is_excluded" not in result.columns
