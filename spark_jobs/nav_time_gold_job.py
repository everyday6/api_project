"""
EMR Serverless 잡 엔트리포인트 — Silver2(LION 세그먼트 단위) -> type1(시간) RDS upsert

nav_time_silver_job.py가 만들어둔 Silver2 parquet을 읽어서 Gold1(필터)
-> Gold2(버킷 평균+시간 계산+RDS upsert)를 처리한다
(docs/superpowers/specs/2026-08-24-split-silver-gold-tasks-design.md 참고).

인자:
  --silver2-path  : nav_time_silver_job.py가 저장한 Silver2 parquet 경로
  --dim-segment-path : LION Gold2 dim_segment.parquet 경로
                       (segment_id, geometry, is_routable, length_ft)
  --serving-table  : upsert할 RDS 테이블명
  --output-s3      : 처리 결과({"count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.nav_time.gold1 import filter_valid_speed
from src.nav_time.gold2 import compute_time_seconds, to_serving_items, write_to_rds


def run(silver2_path: str, dim_segment_path: str, serving_table: str, output_s3: str) -> None:
    spark = SparkSession.builder.appName("nav-time-gold").getOrCreate()

    try:
        silver2_df = spark.read.parquet(silver2_path)
        dim_segment_df = pd.read_parquet(dim_segment_path)

        gold1_df = filter_valid_speed(silver2_df)

        bucket_df = compute_time_seconds(gold1_df, dim_segment_df[["segment_id", "length_ft"]])
        items = to_serving_items(bucket_df, serving_table)
        count = write_to_rds(items, serving_table)
    finally:
        # spark.stop()을 안 부른다 - 테스트가 module-scope fixture를 여러 테스트에서
        # 재사용하는데, 여기서 stop하면 다음 테스트가 죽은 세션을 쓰게 된다.
        # EMR Serverless에서는 job이 별도 프로세스라 종료 시 리소스가 정리되므로 문제없다.
        pass

    S3Path(output_s3).write_text(json.dumps({"count": count}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver2-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--serving-table", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    run(
        silver2_path=args.silver2_path,
        dim_segment_path=args.dim_segment_path,
        serving_table=args.serving_table,
        output_s3=args.output_s3,
    )


if __name__ == "__main__":
    main()
