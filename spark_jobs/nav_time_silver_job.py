"""
EMR Serverless 잡 엔트리포인트 — 속도 Bronze -> Silver2(LION 세그먼트 단위)

Bronze -> Silver1(정제) -> Silver2(LION 세그먼트 매핑)까지만 처리하고
결과를 S3에 parquet로 남긴다. 이어지는 Gold1/Gold2는
nav_time_gold_job.py가 이 결과를 읽어서 별도 EMR job으로 처리한다 -
하나로 묶여있던 job을 Silver/Gold 두 Airflow task로 나눠서, 실패했을 때
Airflow 화면에서 바로 어느 단계인지 알 수 있게 하기 위함
(docs/superpowers/specs/2026-08-24-split-silver-gold-tasks-design.md 참고).

인자:
  --speed-bronze-path : 속도 Bronze parquet 경로(Airflow가 수집한 원본, 정제 전)
  --dim-segment-path   : LION Gold2 dim_segment.parquet 경로
                         (segment_id, geometry, is_routable, length_ft)
  --silver2-output      : Silver2 결과를 저장할 parquet 경로(다음 EMR job이 읽음)
  --output-s3           : 처리 결과({"row_count": N})를 JSON으로 쓸 S3 경로
"""

import argparse
import json

import pandas as pd
from cloudpathlib import S3Path
from pyspark.sql import SparkSession

from src.silver2.segment_speed import build_segment_speed_silver2
from src.speed.silver1 import clean_speed_silver1


def run(speed_bronze_path: str, dim_segment_path: str, silver2_output: str, output_s3: str) -> None:
    spark = SparkSession.builder.appName("nav-time-silver").getOrCreate()

    bronze_df = spark.read.parquet(speed_bronze_path)
    dim_segment_df = pd.read_parquet(dim_segment_path)

    speed_silver1_df = clean_speed_silver1(bronze_df)
    silver2_df = build_segment_speed_silver2(speed_silver1_df, dim_segment_df)

    row_count = silver2_df.count()
    silver2_df.write.parquet(silver2_output, mode="overwrite")

    S3Path(output_s3).write_text(json.dumps({"row_count": row_count}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed-bronze-path", required=True)
    parser.add_argument("--dim-segment-path", required=True)
    parser.add_argument("--silver2-output", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    run(
        speed_bronze_path=args.speed_bronze_path,
        dim_segment_path=args.dim_segment_path,
        silver2_output=args.silver2_output,
        output_s3=args.output_s3,
    )


if __name__ == "__main__":
    main()
