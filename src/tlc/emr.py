"""TLC Spark 연산을 EMR Serverless에 제출하는 Airflow 측 헬퍼."""

from __future__ import annotations

import json
from uuid import uuid4

from src.common.config import EMR_JOBS_DIR, PROJECT_ROOT
from src.common.emr_serverless import read_json_result, run_spark_job


def run_tlc_emr_operation(
    operation: str, payload: dict, max_executors: int | None = None
) -> dict:
    """하나의 TLC 연산을 EMR Serverless에서 실행하고 JSON 결과를 반환한다."""

    run_id = uuid4().hex
    job_name = f"tlc-{operation.replace('_', '-')}-{run_id}"
    output_s3 = EMR_JOBS_DIR / "outputs" / f"{job_name}.json"

    run_spark_job(
        job_name=job_name,
        entry_point_script=PROJECT_ROOT / "spark_jobs" / "tlc_pipeline_job.py",
        entry_point_args=[
            "--operation",
            operation,
            "--payload-json",
            json.dumps(payload, ensure_ascii=False),
            "--output-s3",
            str(output_s3),
        ],
        max_executors=max_executors,
    )
    return read_json_result(str(output_s3))
