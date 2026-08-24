"""
EMR Serverless 잡 엔트리포인트 — LION dim_segment -> type2(길이) RDS upsert

인자:
  --dim-segment-path : LION Gold2 dim_segment.parquet 경로 (s3:// 또는 로컬)
  --serving-table     : upsert할 RDS 테이블명(과거 --dynamodb-table)
  --output-s3         : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_length.gold1 import filter_routable_segments
from src.nav_length.gold2 import to_serving_items, write_to_rds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--serving-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nav-length-gold").getOrCreate()

    try:
        df = spark.read.parquet(args.dim_segment_path)
        filtered = filter_routable_segments(df)
        items = to_serving_items(filtered)
        count = write_to_rds(items, args.serving_table)
    finally:
        spark.stop()

    S3Path(args.output_s3).write_text(json.dumps({"count": count}))


if __name__ == "__main__":
    main()
