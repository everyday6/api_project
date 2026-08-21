"""
EMR Serverless Spark 잡 제출/대기 헬퍼

Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다.
src/ 전체를 zip으로 묶어 --py-files로 넘겨서, 잡 스크립트가 src.tlc.* 등
기존 순수 변환 함수를 그대로 import해서 쓸 수 있게 한다 — 변환 로직을
spark_jobs 쪽에 복제하지 않기 위함이다.
"""

from __future__ import annotations

import json
import time
import uuid
import zipfile
from pathlib import Path

import boto3
from cloudpathlib import S3Path

from src.common.config import (
    AWS_REGION,
    EMR_APPLICATION_ID,
    EMR_JOB_ROLE_ARN,
    EMR_JOBS_DIR,
    PROJECT_ROOT,
    TMP_DIR,
)
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="emr_serverless")

# EMR Serverless 배치 잡은 몇 분 단위로 걸리는 게 보통이라, 상태를 너무
# 자주 조회해서 API를 낭비할 필요가 없다.
_POLL_INTERVAL_SECONDS = 15
_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}


def _upload_src_bundle() -> str:
    """src/ 디렉터리를 zip으로 묶어 EMR_JOBS_DIR에 올리고 경로를 반환한다.

    잡마다 매번 새로 올린다 — src/가 몇백 KB 수준이라 비용/시간 부담이
    거의 없고, 코드가 바뀐 채로 캐시된 옛 zip을 잘못 쓰는 사고를 막는다.
    """
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = TMP_DIR / f"emr_src_bundle_{uuid.uuid4().hex}.zip"

    src_dir = PROJECT_ROOT / "src"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in src_dir.rglob("*.py"):
            zf.write(path, arcname=path.relative_to(PROJECT_ROOT))

    dest = EMR_JOBS_DIR / "bundles" / "src.zip"

    if isinstance(dest, Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(zip_path.read_bytes())
    else:
        dest.upload_from(zip_path)

    zip_path.unlink()

    return str(dest)


def _upload_script(local_path: Path, job_name: str) -> str:
    """잡 엔트리포인트 스크립트를 EMR_JOBS_DIR에 올리고 경로를 반환한다."""
    dest = EMR_JOBS_DIR / "scripts" / f"{job_name}.py"

    if isinstance(dest, Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(local_path.read_bytes())
    else:
        dest.upload_from(local_path)

    return str(dest)


def run_spark_job(
    job_name: str,
    entry_point_script: Path,
    entry_point_args: list[str],
) -> None:
    """EMR Serverless에 Spark 잡을 제출하고 끝날 때까지 기다린다.

    실패(FAILED/CANCELLED)면 예외를 던져 Airflow가 기존 재시도/Slack
    실패 알림 경로를 그대로 타게 한다.
    """
    client = boto3.client("emr-serverless", region_name=AWS_REGION)

    src_bundle_s3 = _upload_src_bundle()
    entry_point_s3 = _upload_script(entry_point_script, job_name)

    logger.info(f"EMR Serverless 잡 제출: {job_name}")

    response = client.start_job_run(
        applicationId=EMR_APPLICATION_ID,
        executionRoleArn=EMR_JOB_ROLE_ARN,
        name=job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point_s3,
                "entryPointArguments": entry_point_args,
                "sparkSubmitParameters": f"--py-files {src_bundle_s3}",
            }
        },
    )

    job_run_id = response["jobRunId"]

    logger.info(f"EMR Serverless 잡 실행 중: {job_name} (jobRunId={job_run_id})")

    while True:
        time.sleep(_POLL_INTERVAL_SECONDS)

        job_run = client.get_job_run(
            applicationId=EMR_APPLICATION_ID,
            jobRunId=job_run_id,
        )["jobRun"]

        state = job_run["state"]

        if state in _TERMINAL_STATES:
            break

    if state != "SUCCESS":
        raise RuntimeError(
            f"EMR Serverless 잡 실패: {job_name} "
            f"(jobRunId={job_run_id}, state={state}, "
            f"detail={job_run.get('stateDetails')})"
        )

    logger.info(f"EMR Serverless 잡 완료: {job_name} (jobRunId={job_run_id})")


def read_json_result(s3_path: str):
    """잡이 저장해둔 JSON 결과 파일을 읽는다."""
    return json.loads(S3Path(s3_path).read_text())
