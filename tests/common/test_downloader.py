from datetime import datetime
from zoneinfo import ZoneInfo

from src.common.downloader import get_recent_service_months


def test_recent_service_months_include_candidate_and_three_complete_months():
    reference_time = datetime(
        2026,
        8,
        22,
        12,
        0,
        tzinfo=ZoneInfo("America/New_York"),
    )

    months = get_recent_service_months(reference_time)

    assert [value.strftime("%Y-%m") for value in months] == [
        "2026-06",
        "2026-05",
        "2026-04",
        "2026-03",
    ]
    assert all(value.day == 1 for value in months)


def test_recent_service_months_cross_year_boundary():
    reference_time = datetime(
        2026,
        2,
        1,
        tzinfo=ZoneInfo("America/New_York"),
    )

    months = get_recent_service_months(reference_time)

    assert [value.strftime("%Y-%m") for value in months] == [
        "2025-12",
        "2025-11",
        "2025-10",
        "2025-09",
    ]
