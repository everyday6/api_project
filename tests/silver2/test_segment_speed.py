from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.silver2.segment_speed import build_segment_speed_silver2


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("segment_speed_test").getOrCreate()
    yield session
    session.stop()


def test_build_segment_speed_silver2_expands_link_to_segments(spark, monkeypatch):
    import src.silver2.segment_speed as module

    # link-1이 seg-1, seg-2 두 개에 매핑된다고 가정 -> 판독값 1개가 2행으로 펼쳐져야 함
    monkeypatch.setattr(
        module,
        "match_links_to_segments",
        lambda links_df, dim_segment_df: pd.DataFrame([
            {"link_id": "link-1", "segment_id": "seg-1", "distance_ft": 10.0, "mapping_method": "buffer"},
            {"link_id": "link-1", "segment_id": "seg-2", "distance_ft": 20.0, "mapping_method": "buffer"},
        ]),
    )

    speed_silver1_df = spark.createDataFrame([
        {"link_id": "link-1", "link_points": "40.7,-74.0 40.71,-74.01", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_df = pd.DataFrame([{"segment_id": "seg-1", "geometry": "x"}])

    result = build_segment_speed_silver2(speed_silver1_df, dim_segment_df).collect()

    assert sorted(r["segment_id"] for r in result) == ["seg-1", "seg-2"]
    assert all(r["speed"] == 30.0 for r in result)


def test_build_segment_speed_silver2_unmatched_link_produces_no_rows(spark, monkeypatch):
    import src.silver2.segment_speed as module

    monkeypatch.setattr(
        module, "match_links_to_segments", lambda links_df, dim_segment_df: pd.DataFrame(
            columns=["link_id", "segment_id", "distance_ft", "mapping_method"]
        ),
    )

    speed_silver1_df = spark.createDataFrame([
        {"link_id": "link-unmatched", "link_points": "40.7,-74.0 40.71,-74.01", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_df = pd.DataFrame([{"segment_id": "seg-1", "geometry": "x"}])

    result = build_segment_speed_silver2(speed_silver1_df, dim_segment_df).collect()

    assert result == []
