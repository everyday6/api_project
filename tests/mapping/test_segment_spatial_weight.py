import pandas as pd
import pytest

from src.mapping.segment_spatial_weight import ingest_hotspot_grid


def test_ingest_hotspot_grid_copies_columns_and_adds_metadata(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({
        "lat_bin": [40.75, 40.76],
        "lon_bin": [-73.98, -73.97],
        "dropoff_count": [100, 50],
    }).to_csv(source_csv, index=False)

    bronze_path = tmp_path / "bronze" / "dropoff_grid.parquet"
    out_path = ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=bronze_path)

    assert out_path == str(bronze_path)
    df = pd.read_parquet(bronze_path)
    assert len(df) == 2
    assert list(df["lat_bin"]) == [40.75, 40.76]
    assert list(df["dropoff_count"]) == [100, 50]
    assert (df["_source"] == "bq_2016_dropoff_grid").all()
    assert df["_ingested_at"].notna().all()


def test_ingest_hotspot_grid_missing_column_raises(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({"lat_bin": [40.75], "lon_bin": [-73.98]}).to_csv(source_csv, index=False)

    with pytest.raises(ValueError, match="필수 컬럼"):
        ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=tmp_path / "out.parquet")
