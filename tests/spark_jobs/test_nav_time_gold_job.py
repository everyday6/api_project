import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from spark_jobs import nav_time_gold_job


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nav_time_gold_job_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_run_calls_gold_functions_in_order_and_writes_output(tmp_path, spark):
    silver2_path = tmp_path / "silver2.parquet"
    spark.createDataFrame(
        [{"segment_id": "seg-1", "speed": 29.82, "observed_at": "2026-08-24T12:00:00"}]
    ).write.parquet(str(silver2_path))

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame([{"segment_id": "seg-1", "length_ft": 500.0}]).to_parquet(dim_segment_path, index=False)

    output_s3 = tmp_path / "result.json"
    serving_table = "test_segment_metrics_type1"

    fake_gold1_df = spark.createDataFrame([{"segment_id": "seg-1", "speed": 29.82}])
    fake_bucket_df = spark.createDataFrame([{"segment_id": "seg-1", "bucket": "1200", "time_seconds": 61.0}])
    fake_items = [{"segment_id": "seg-1", "time": "1200", "value": 61}]

    with patch.object(nav_time_gold_job, "S3Path", Path), \
         patch.object(nav_time_gold_job, "filter_valid_speed", return_value=fake_gold1_df) as mock_gold1, \
         patch.object(
             nav_time_gold_job, "compute_time_seconds", return_value=fake_bucket_df
         ) as mock_gold2, \
         patch.object(
             nav_time_gold_job, "to_serving_items", return_value=fake_items
         ) as mock_to_items, \
         patch.object(nav_time_gold_job, "write_to_rds", return_value=1) as mock_write:
        nav_time_gold_job.run(
            silver2_path=str(silver2_path),
            dim_segment_path=str(dim_segment_path),
            serving_table=serving_table,
            output_s3=str(output_s3),
        )

    # filter_valid_speed는 Silver2 DataFrame 하나만 받는다.
    mock_gold1.assert_called_once()
    silver2_df_arg = mock_gold1.call_args.args[0]
    assert silver2_df_arg.collect()[0]["segment_id"] == "seg-1"

    # compute_time_seconds는 filter_valid_speed 결과 + (segment_id, length_ft) 컬럼만 받는다.
    mock_gold2.assert_called_once()
    gold1_arg, length_df_arg = mock_gold2.call_args.args
    assert gold1_arg is fake_gold1_df
    assert length_df_arg.columns.tolist() == ["segment_id", "length_ft"]

    # to_serving_items는 compute_time_seconds 결과 + 테이블명을 받는다.
    mock_to_items.assert_called_once_with(fake_bucket_df, serving_table)

    # write_to_rds는 to_serving_items 결과 + 테이블명을 받는다.
    mock_write.assert_called_once_with(fake_items, serving_table)

    result = json.loads(Path(output_s3).read_text())
    assert result == {"count": 1}


def test_main_requires_all_four_arguments():
    with patch("sys.argv", ["nav_time_gold_job.py"]):
        with pytest.raises(SystemExit):
            nav_time_gold_job.main()
