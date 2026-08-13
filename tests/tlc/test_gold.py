from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.tlc.gold import _expand_zone_to_segment_hour, _normalize_tlc_volume, _read_zone_hour_counts


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tlc_gold_test").getOrCreate()
    yield session
    session.stop()


def _write_tlc_silver_fixture(base_dir, taxi_type, month, rows):
    """base_dir/{taxi_type}_tripdata_{month}/data.parquet 형태로 TLC silver 픽스처를 만든다.

    coerce_timestamps="us"는 이 테스트 환경의 pyarrow(21.x)가 pandas
    datetime64[ns] 컬럼을 기본값으로 Parquet TIMESTAMP(NANOS)로 쓰기 때문에
    필요하다. 실제 운영 TLC silver 파일은 Spark 자체가 마이크로초 단위로
    쓰므로 이 문제가 없다 — 이 fixture 헬퍼에만 해당하는 환경 특이사항.
    """
    out_dir = base_dir / f"{taxi_type}_tripdata_{month}"
    out_dir.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(
        out_dir / "data.parquet",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )


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


def test_read_zone_hour_counts_filters_weekday_and_counts(tmp_path, spark):
    # 2024-01-01(월)은 포함, 2024-01-06(토)은 제외되어야 한다.
    rows = [
        {
            "pickup_datetime": datetime(2024, 1, 1, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 8, 30),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 1, 8, 10),
            "dropoff_datetime": datetime(2024, 1, 1, 8, 45),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 2.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 6, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 6, 8, 30),
            "pickup_location_id": 10,
            "dropoff_location_id": 5,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
    ]
    _write_tlc_silver_fixture(tmp_path, "yellow", "2024-01", rows)

    result = _read_zone_hour_counts(spark, silver_dir=tmp_path, taxi_types=["yellow"])

    assert len(result) == 1
    row = result.iloc[0]
    assert row["zone_id"] == 5
    assert row["hour"] == 8
    assert row["dropoff_count"] == 2  # 월요일 2건만 카운트, 토요일 제외


def test_read_zone_hour_counts_reads_multiple_taxi_types(tmp_path, spark):
    _write_tlc_silver_fixture(tmp_path, "yellow", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 2, 9, 0),
        "dropoff_datetime": datetime(2024, 1, 2, 9, 15),
        "pickup_location_id": 1,
        "dropoff_location_id": 7,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])
    _write_tlc_silver_fixture(tmp_path, "green", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 2, 9, 5),
        "dropoff_datetime": datetime(2024, 1, 2, 9, 20),
        "pickup_location_id": 2,
        "dropoff_location_id": 7,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])

    result = _read_zone_hour_counts(spark, silver_dir=tmp_path, taxi_types=["yellow", "green"])

    assert len(result) == 1
    row = result.iloc[0]
    assert row["zone_id"] == 7
    assert row["hour"] == 9
    assert row["dropoff_count"] == 2  # yellow 1건 + green 1건


from src.tlc.gold import build_dim_segment_tlc_volume, validate_dim_segment_tlc_volume


def test_build_and_validate_dim_segment_tlc_volume(tmp_path, spark):
    # 세그먼트 A,B는 zone 1(맨해튼), 세그먼트 C는 zone 2(맨해튼).
    # 세그먼트 D는 zone 1이지만 브루클린이라 결과에서 제외되어야 한다.
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B", "C", "D"],
        "zone_id": [1, 1, 2, 1],
        "borough": ["Manhattan", "Manhattan", "Manhattan", "Brooklyn"],
    }).to_parquet(map_zone_segment_path, index=False)

    silver_dir = tmp_path / "silver"
    _write_tlc_silver_fixture(silver_dir, "yellow", "2024-01", [{
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "pickup_location_id": 1,
        "dropoff_location_id": 1,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    }])

    out_path = build_dim_segment_tlc_volume(
        spark,
        map_zone_segment_path=map_zone_segment_path,
        silver_dir=silver_dir,
        taxi_types=["yellow"],
    )

    validated_path = validate_dim_segment_tlc_volume(out_path, map_zone_segment_path=map_zone_segment_path)
    assert validated_path == out_path

    df = pd.read_parquet(out_path)
    assert len(df) == 3 * 24  # 맨해튼 세그먼트(A,B,C)만 — D는 제외
    assert "D" not in df["segment_id"].values
    hour8 = df[df["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == 1
    assert hour8["B"] == 1
    assert hour8["C"] == 0


def test_validate_dim_segment_tlc_volume_rejects_duplicate_rows(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A"],
        "zone_id": [1],
        "borough": ["Manhattan"],
    }).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "dim_segment_tlc_volume.parquet"
    pd.DataFrame({
        "segment_id": ["A"] * 25,  # 24개여야 하는데 25개 (중복)
        "hour": list(range(24)) + [0],
        "dropoff_count_raw": [0] * 25,
        "tlc_volume": [0.5] * 25,
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError):
        validate_dim_segment_tlc_volume(str(bad_path), map_zone_segment_path=map_zone_segment_path)
