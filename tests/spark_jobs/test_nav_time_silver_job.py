import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from spark_jobs import nav_time_silver_job


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nav_time_silver_job_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_calls_silver_functions_in_order_and_writes_output(tmp_path, spark):
    bronze_path = tmp_path / "bronze.parquet"
    pd.DataFrame([{"link_id": "1", "speed": "29.82", "data_as_of": "2026-08-24T12:00:00.000"}]).to_parquet(
        bronze_path, index=False
    )

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame([{"segment_id": "seg-1", "length_ft": 500.0}]).to_parquet(dim_segment_path, index=False)

    silver2_output = str(tmp_path / "silver2.parquet")
    output_s3 = tmp_path / "result.json"

    fake_silver1_df = spark.createDataFrame([{"link_id": "1"}])
    fake_silver2_df = spark.createDataFrame(
        [{"segment_id": "seg-1", "speed": 29.82, "observed_at": "2026-08-24T12:00:00"}]
    )

    with patch.object(nav_time_silver_job, "S3Path", Path), \
         patch.object(
             nav_time_silver_job, "clean_speed_silver1", return_value=fake_silver1_df
         ) as mock_silver1, \
         patch.object(
             nav_time_silver_job, "build_segment_speed_silver2", return_value=fake_silver2_df
         ) as mock_silver2:
        nav_time_silver_job.run(
            speed_bronze_path=str(bronze_path),
            dim_segment_path=str(dim_segment_path),
            silver2_output=silver2_output,
            output_s3=str(output_s3),
        )

    # clean_speed_silver1은 Bronze DataFrame 하나만 받는다.
    mock_silver1.assert_called_once()
    bronze_df_arg = mock_silver1.call_args.args[0]
    assert bronze_df_arg.collect()[0]["link_id"] == "1"

    # build_segment_speed_silver2는 clean_speed_silver1의 결과 + dim_segment_df를 받는다.
    mock_silver2.assert_called_once()
    silver1_arg, dim_segment_arg = mock_silver2.call_args.args
    assert silver1_arg is fake_silver1_df
    assert dim_segment_arg["segment_id"].tolist() == ["seg-1"]

    saved = spark.read.parquet(silver2_output)
    assert saved.count() == 1
    assert saved.collect()[0]["segment_id"] == "seg-1"

    result = json.loads(Path(output_s3).read_text())
    assert result == {"row_count": 1}


def test_main_requires_all_four_arguments():
    with patch("sys.argv", ["nav_time_silver_job.py"]):
        with pytest.raises(SystemExit):
            nav_time_silver_job.main()
