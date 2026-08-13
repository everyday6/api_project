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


def test_read_zone_hour_counts_drops_null_zone_id(tmp_path, spark, caplog):
    # 실 데이터(SILVER_SCHEMA)는 dropoff_location_id가 nullable이고 결측치를
    # 삭제하지 않는다. group by는 NULL도 자기 그룹으로 유지하므로, 이 결측
    # 트립이 있어도 크래시 없이 제외되고 나머지는 정상 집계돼야 한다.
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
            "pickup_datetime": datetime(2024, 1, 1, 9, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 9, 30),
            "pickup_location_id": 10,
            "dropoff_location_id": None,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 1, 9, 5),
            "dropoff_datetime": datetime(2024, 1, 1, 9, 40),
            "pickup_location_id": 10,
            "dropoff_location_id": None,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
    ]
    df = pd.DataFrame(rows)
    # astype("Int32")로 캐스팅해야 None이 float64/NaN이 아니라 실제 운영
    # 스키마(IntegerType, nullable)와 같은 nullable-int로 parquet에 저장된다.
    df["dropoff_location_id"] = df["dropoff_location_id"].astype("Int32")

    out_dir = tmp_path / "yellow_tripdata_2024-01"
    out_dir.mkdir(parents=True)
    df.to_parquet(
        out_dir / "data.parquet",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    with caplog.at_level("WARNING"):
        result = _read_zone_hour_counts(spark, silver_dir=tmp_path, taxi_types=["yellow"])

    # 결측 zone 트립(2건)은 제외되고, zone_id=5 트립(1건)만 남아야 한다.
    assert len(result) == 1
    row = result.iloc[0]
    assert row["zone_id"] == 5
    assert row["hour"] == 8
    assert row["dropoff_count"] == 1
    assert not result["zone_id"].isna().any()

    assert any("결측" in rec.message and "2건" in rec.message for rec in caplog.records)


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


from src.tlc.gold import (
    build_dim_segment_tlc_volume,
    validate_dim_segment_tlc_volume,
    _neighbor_hop_distances,
    get_tlc_traffic_score_for_construction,
)


def test_neighbor_hop_distances_walks_graph():
    # 양방향 그래프: A-B, A-C, B-D
    adjacency = pd.DataFrame({
        "segment_id":          ["A", "B", "A", "C", "B", "D"],
        "neighbor_segment_id": ["B", "A", "C", "A", "D", "B"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=3)

    assert result == {"A": 0, "B": 1, "C": 1, "D": 2}


def test_neighbor_hop_distances_respects_hop_limit():
    # A-B-C 체인
    adjacency = pd.DataFrame({
        "segment_id":          ["A", "B", "B", "C"],
        "neighbor_segment_id": ["B", "A", "C", "B"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=1)

    assert result == {"A": 0, "B": 1}  # C는 2단계라 hops=1이면 제외


def test_neighbor_hop_distances_isolated_segment():
    adjacency = pd.DataFrame({
        "segment_id": ["X", "Y"],
        "neighbor_segment_id": ["Y", "X"],
    })

    result = _neighbor_hop_distances("A", adjacency, hops=3)  # A는 그래프에 없음

    assert result == {"A": 0}


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

    # 픽스처가 세그먼트 3개짜리라 min_segments/max_segments의 실 운영 기본값
    # (15,000~25,000)을 이 테스트 규모에 맞게 좁혀서 넘긴다.
    validated_path = validate_dim_segment_tlc_volume(
        out_path, map_zone_segment_path=map_zone_segment_path, min_segments=1, max_segments=10,
    )
    assert validated_path == out_path

    df = pd.read_parquet(out_path)
    assert len(df) == 3 * 24  # 맨해튼 세그먼트(A,B,C)만 — D는 제외
    assert "D" not in df["segment_id"].values
    hour8 = df[df["hour"] == 8].set_index("segment_id")["dropoff_count_raw"]
    assert hour8["A"] == 1
    assert hour8["B"] == 1
    assert hour8["C"] == 0


def test_build_dim_segment_tlc_volume_logs_unmatched_zone_trips(tmp_path, spark, caplog):
    # zone 99는 map_zone_segment 어디에도 없다(TLC 특수 zone 코드나 다른
    # 자치구 zone을 흉내). 이런 트립은 결과에서 조용히 빠지되, 몇 건이
    # 빠졌는지는 로그로 남아야 한다(spec의 "제외 대상" 항목).
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A"],
        "zone_id": [1],
        "borough": ["Manhattan"],
    }).to_parquet(map_zone_segment_path, index=False)

    silver_dir = tmp_path / "silver"
    _write_tlc_silver_fixture(silver_dir, "yellow", "2024-01", [
        {
            "pickup_datetime": datetime(2024, 1, 1, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 8, 30),
            "pickup_location_id": 1,
            "dropoff_location_id": 1,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
        {
            "pickup_datetime": datetime(2024, 1, 1, 9, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 9, 30),
            "pickup_location_id": 1,
            "dropoff_location_id": 99,  # 매칭 안 되는 zone
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        },
    ])

    with caplog.at_level("WARNING"):
        out_path = build_dim_segment_tlc_volume(
            spark,
            map_zone_segment_path=map_zone_segment_path,
            silver_dir=silver_dir,
            taxi_types=["yellow"],
        )

    assert any("매칭되지 않아 제외된 하차 1건" in rec.message for rec in caplog.records)

    df = pd.read_parquet(out_path)
    assert df["dropoff_count_raw"].sum() == 1  # zone 99의 트립은 결과에 안 들어감


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


def test_validate_dim_segment_tlc_volume_rejects_zero_matching_segments(tmp_path, spark):
    # borough 표기 오타 등으로 map_zone_segment에 borough="Manhattan"인 세그먼트가
    # 하나도 없는 경우. build는 빈 Gold 테이블을 그대로 써버리고, 이런 상황에서도
    # validate가 (0 duplicates, 0 rows 다 between() 만족, 0 == 0*24) 식으로
    # 통과해버리면 안 된다 — segment_count > 0 체크가 이를 막아야 한다.
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B"],
        "zone_id": [1, 2],
        "borough": ["Brooklyn", "Queens"],  # Manhattan이 하나도 없음
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

    # build 자체는 (의도된 대로) 빈 Gold 테이블을 조용히 써낸다 — 문제는 validate.
    assert len(pd.read_parquet(out_path)) == 0

    with pytest.raises(AssertionError, match="세그먼트가 없습니다"):
        validate_dim_segment_tlc_volume(out_path, map_zone_segment_path=map_zone_segment_path)


@pytest.fixture
def gold_and_adjacency_paths(tmp_path):
    gold_path = tmp_path / "dim_segment_tlc_volume.parquet"
    pd.DataFrame({
        "segment_id":        ["A", "A", "B", "B", "C", "C"],
        "hour":               [8,   9,   8,   9,   8,   9],
        "dropoff_count_raw": [10,  20,   5,   5,   1,   1],
        "tlc_volume":        [0.9, 0.95, 0.5, 0.5, 0.1, 0.1],
    }).to_parquet(gold_path, index=False)

    # D는 A의 직접 이웃으로 그래프에는 있지만, Gold 테이블(맨해튼 한정)에는
    # 아예 없다 — 맨해튼 밖으로 벗어난 이웃을 흉내낸 것. 이런 이웃은 결과에서
    # 조용히 빠져야 한다(에러도, null score도 없이).
    adjacency_path = tmp_path / "graph_segment_adjacency.parquet"
    pd.DataFrame({
        "segment_id":          ["A", "B", "A"],
        "neighbor_segment_id": ["B", "A", "D"],
    }).to_parquet(adjacency_path, index=False)

    return gold_path, adjacency_path


def test_get_tlc_traffic_score_for_construction_returns_self_and_neighbors(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    result = get_tlc_traffic_score_for_construction(
        "A", hour=8, hops=3, gold_path=gold_path, adjacency_path=adjacency_path,
    )

    by_segment = {r["segment_id"]: r for r in result}
    assert by_segment["A"] == {"segment_id": "A", "hop_distance": 0, "hour": 8, "traffic_score": 0.9}
    assert by_segment["B"] == {"segment_id": "B", "hop_distance": 1, "hour": 8, "traffic_score": 0.5}
    assert "C" not in by_segment  # A와 인접하지 않음
    assert [r["segment_id"] for r in result] == ["A", "B"]  # hop_distance 오름차순


def test_get_tlc_traffic_score_for_construction_excludes_neighbor_missing_from_gold(gold_and_adjacency_paths):
    # D는 A의 직접(1-hop) 이웃이지만 Gold 테이블(맨해튼 한정)에는 없다.
    # Gold가 시티 전체가 아니라 맨해튼만 담고 있어 생기는 정상적인 상황이므로
    # 에러 없이, null score도 없이 결과에서 조용히 빠져야 한다.
    gold_path, adjacency_path = gold_and_adjacency_paths

    result = get_tlc_traffic_score_for_construction(
        "A", hour=8, hops=3, gold_path=gold_path, adjacency_path=adjacency_path,
    )

    segment_ids = [r["segment_id"] for r in result]
    assert "D" not in segment_ids
    assert segment_ids == ["A", "B"]  # D는 조용히 제외되고 A, B만 남음


def test_get_tlc_traffic_score_for_construction_missing_segment_raises(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    with pytest.raises(KeyError):
        get_tlc_traffic_score_for_construction(
            "Z", hour=8, gold_path=gold_path, adjacency_path=adjacency_path,
        )


def test_get_tlc_traffic_score_for_construction_invalid_hour_raises(gold_and_adjacency_paths):
    gold_path, adjacency_path = gold_and_adjacency_paths

    with pytest.raises(ValueError):
        get_tlc_traffic_score_for_construction(
            "A", hour=24, gold_path=gold_path, adjacency_path=adjacency_path,
        )


def test_build_then_query_full_pipeline_seam(tmp_path, spark):
    """build_dim_segment_tlc_volume이 실제로 써낸 Gold를 get_tlc_traffic_score_for_construction이
    그대로 읽어서 맞는 값을 돌려주는지 — 두 함수를 잇는 이음매 자체를 검증한다.
    (기존 테스트는 build/validate와 query를 각각 따로만 테스트했다.)

    세그먼트 A,B는 zone 1(같은 zone 공유), C는 zone 2 — 전부 맨해튼. 인접
    그래프는 A-B만 연결한다.
    """
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
        "borough": ["Manhattan", "Manhattan", "Manhattan"],
    }).to_parquet(map_zone_segment_path, index=False)

    adjacency_path = tmp_path / "graph_segment_adjacency.parquet"
    pd.DataFrame({
        "segment_id":          ["A", "B"],
        "neighbor_segment_id": ["B", "A"],
    }).to_parquet(adjacency_path, index=False)

    # zone 1 하차: 8시 5건, 9시 2건. zone 2 하차: 8시 1건. 나머지 시간대는 0건.
    rows = []
    for minute in range(5):
        rows.append({
            "pickup_datetime": datetime(2024, 1, 1, 8, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 8, minute),
            "pickup_location_id": 1,
            "dropoff_location_id": 1,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        })
    for minute in range(2):
        rows.append({
            "pickup_datetime": datetime(2024, 1, 1, 9, 0),
            "dropoff_datetime": datetime(2024, 1, 1, 9, minute),
            "pickup_location_id": 1,
            "dropoff_location_id": 1,
            "passenger_count": 1.0,
            "trip_distance": 1.0,
        })
    rows.append({
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropoff_datetime": datetime(2024, 1, 1, 8, 50),
        "pickup_location_id": 2,
        "dropoff_location_id": 2,
        "passenger_count": 1.0,
        "trip_distance": 1.0,
    })

    silver_dir = tmp_path / "silver"
    _write_tlc_silver_fixture(silver_dir, "yellow", "2024-01", rows)

    out_path = build_dim_segment_tlc_volume(
        spark,
        map_zone_segment_path=map_zone_segment_path,
        silver_dir=silver_dir,
        taxi_types=["yellow"],
    )
    validate_dim_segment_tlc_volume(
        out_path, map_zone_segment_path=map_zone_segment_path, min_segments=1, max_segments=10,
    )

    # 3개 세그먼트 x 24시간 = 72행. dropoff_count_raw 분포: 0이 67행, 1이 1행
    # (C@8시), 2가 2행(A,B@9시), 5가 2행(A,B@8시). rank(pct=True, method="average")로
    # 손으로 기대값을 계산한다 — 이 테스트는 이 계산을 재구현하는 게 아니라, build가
    # 써낸 실제 값을 query 함수가 그대로 돌려주는지가 목적이라 build 함수를 다시
    # 부르지 않고 직접 산수로만 기대값을 낸다.
    expected_score_8h = (71 + 72) / 2 / 72  # A,B@8시=5건: 공동 71,72등
    expected_score_9h = (69 + 70) / 2 / 72  # A,B@9시=2건: 공동 69,70등

    result_8h = get_tlc_traffic_score_for_construction(
        "A", hour=8, hops=1, gold_path=out_path, adjacency_path=adjacency_path,
    )
    by_segment = {r["segment_id"]: r for r in result_8h}
    assert set(by_segment) == {"A", "B"}  # C는 A와 인접하지 않아 빠짐
    assert by_segment["A"]["hop_distance"] == 0
    assert by_segment["B"]["hop_distance"] == 1
    assert by_segment["A"]["traffic_score"] == pytest.approx(expected_score_8h)
    assert by_segment["B"]["traffic_score"] == pytest.approx(expected_score_8h)

    result_9h = get_tlc_traffic_score_for_construction(
        "A", hour=9, hops=1, gold_path=out_path, adjacency_path=adjacency_path,
    )
    by_segment_9h = {r["segment_id"]: r for r in result_9h}
    assert by_segment_9h["A"]["traffic_score"] == pytest.approx(expected_score_9h)
    # 8시(트립 5건)가 9시(트립 2건)보다 더 붐빈다는 게 점수에도 그대로 반영돼야 한다.
    assert by_segment["A"]["traffic_score"] > by_segment_9h["A"]["traffic_score"]

    # query 함수가 돌려준 값이 실제로 build가 써낸 Gold 테이블 값 그 자체인지 확인
    # (읽는 경로가 어긋나 있지 않은지 — 이 테스트가 지키려는 핵심 이음매).
    gold_df = pd.read_parquet(out_path)
    actual_cell = gold_df.loc[(gold_df["segment_id"] == "A") & (gold_df["hour"] == 8), "tlc_volume"].iloc[0]
    assert by_segment["A"]["traffic_score"] == pytest.approx(float(actual_cell))
