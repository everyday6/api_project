"""
EMR Serverless 잡 엔트리포인트 — 속도 Bronze -> type1(시간) RDS upsert

Silver1(정제) -> Silver2(LION 매핑) -> Gold1(필터) -> Gold2(버킷 평균+시간
계산+upsert)를 한 잡 안에서 순서대로 수행한다. Airflow는 원본 Bronze
parquet만 이 job에 넘긴다.

인자:
  --speed-bronze-path : 속도 Bronze parquet 경로(Airflow가 수집한 원본, 정제 전)
  --dim-segment-path   : LION Gold2 dim_segment.parquet 경로
                         (segment_id, geometry, is_routable, length_ft)
  --serving-table       : upsert할 RDS 테이블명(과거 --dynamodb-table)
  --output-s3           : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_valid_speed
from src.nav_time.gold2 import compute_time_seconds, to_serving_items, write_to_rds
from src.silver2.segment_speed import build_segment_speed_silver2
from src.speed.silver1 import clean_speed_silver1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed-bronze-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--serving-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nav-time-gold").getOrCreate()

    try:
        bronze_df = spark.read.parquet(args.speed_bronze_path)
        dim_segment_df = pd.read_parquet(args.dim_segment_path)

        speed_silver1_df = clean_speed_silver1(bronze_df)
        silver2_df = build_segment_speed_silver2(speed_silver1_df, dim_segment_df)

        gold1_df = filter_valid_speed(silver2_df)

        bucket_df = compute_time_seconds(gold1_df, dim_segment_df[["segment_id", "length_ft"]])
        items = to_serving_items(bucket_df, args.serving_table)
        count = write_to_rds(items, args.serving_table)
    finally:
        spark.stop()

    S3Path(args.output_s3).write_text(json.dumps({"count": count}))


if __name__ == "__main__":
    main()
