"""
EMR Serverless Spark 잡 제출/대기 헬퍼

Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다.
src/ 전체를 zip으로 묶어 --py-files로 넘겨서, 잡 스크립트가 src.tlc.* 등
기존 순수 변환 함수를 그대로 import해서 쓸 수 있게 한다 — 변환 로직을
spark_jobs 쪽에 복제하지 않기 위함이다.

우리 Spark job(nav_length_job.py, nav_time_job.py 등)은 pandas/geopandas/
shapely/pyproj/cloudpathlib 같은 서드파티 라이브러리도 쓰는데, EMR
Serverless 기본 이미지에는 이게 없다. --py-files는 순수 파이썬 코드만
배포하고 패키지를 설치해주지 않으므로, venv-pack으로 미리 패키징해둔
파이썬 환경(scripts/package_emr_dependencies.sh로 빌드/업로드)을
spark.archives로 같이 실어서 드라이버/executor가 그 환경의 python을
쓰게 한다(AWS 공식 권장 방식). 자세한 배경은
.superpowers/sdd/final-review-c3-report.md 참고.
"""

from __future__ import annotations

import gzip
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
    EMR_PYTHON_ENV_S3_PATH,
    PROJECT_ROOT,
    RDS_DB,
    RDS_HOST,
    RDS_PASSWORD,
    RDS_PORT,
    RDS_USER,
    TMP_DIR,
)
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="emr_serverless")

# EMR Serverless 배치 잡은 몇 분 단위로 걸리는 게 보통이라, 상태를 너무
# 자주 조회해서 API를 낭비할 필요가 없다.
_POLL_INTERVAL_SECONDS = 15
_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}

# EMR Serverless 잡의 드라이버 stdout/stderr를 여기로 떨어뜨리도록 매
# start_job_run에 지정한다. EMR Studio/콘솔 접근 권한이 없어도, 실패 시
# 이 경로에서 로그를 직접 읽어 Airflow 태스크 로그에 그대로 찍어줄 수 있다.
_EMR_LOGS_DIR = EMR_JOBS_DIR / "logs"

# 태스크 로그가 너무 길어지는 걸 막기 위해 드라이버 로그 마지막 N줄만 찍는다.
_LOG_TAIL_LINES = 200


def _validate_rds_env() -> None:
    """RDS_HOST/RDS_DB가 없으면 바로 에러를 낸다.

    EMR Serverless 컨테이너에는 .env 파일이 없어서(src/common/config.py 참고)
    RDS_HOST 등이 os.getenv()로 그냥 두면 전부 None이 된다. 이 값들이 없으면
    잡 안에서 db.py._dsn()이 결국 터지긴 하지만, boto3 클라이언트 생성/S3
    업로드까지 다 하고 EMR에 잡을 제출한 뒤 몇 분 기다렸다가 실패하는 것보다,
    run_spark_job() 맨 앞에서 바로 에러를 내는 게 디버깅이 훨씬 빠르다."""
    if not RDS_HOST or not RDS_DB:
        raise RuntimeError(
            "RDS_HOST/RDS_DB 환경변수가 필요합니다 - .env에 실제 RDS 접속 정보를 채워주세요"
        )


def _rds_env_conf() -> str:
    """RDS(PostgreSQL) 접속 정보를 driver/executor 환경변수로 주입하는 spark-submit
    conf 조각을 만든다. 호출 전에 _validate_rds_env()로 이미 검증됐다고 가정한다.

    nav_time_job.py/nav_length_job.py는 드라이버에서, tlc_pipeline_job.py의
    Type3 롤링 발행은 foreachPartition으로 executor에서 db.py의 write 함수를
    호출하므로 둘 다 필요하다.

    비밀번호를 그대로 커맨드라인 문자열로 넘기므로 EMR 잡 실행 이력(콘솔/
    get-job-run API)에 평문으로 남는다 — PYSPARK_PYTHON 관련 conf와 동일한
    방식을 우선 맞췄지만, 운영에서는 Secrets Manager 조회로 바꾸는 걸 권장한다.
    """
    pairs = {
        "RDS_HOST": RDS_HOST,
        "RDS_PORT": RDS_PORT,
        "RDS_DB": RDS_DB,
        "RDS_USER": RDS_USER,
        "RDS_PASSWORD": RDS_PASSWORD,
    }
    return " ".join(
        f"--conf spark.{scope}Env.{key}={value} "
        for key, value in pairs.items()
        for scope in ("emr-serverless.driver", "executor")
    ).strip()


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


def _fetch_driver_log_tail(job_run_id: str, stream: str) -> str:
    """실패한 잡의 드라이버 로그(stdout/stderr) 마지막 부분을 S3에서 읽어온다.

    EMR Serverless는 s3MonitoringConfiguration으로 지정한 경로 아래
    applications/{appId}/jobs/{jobRunId}/SPARK_DRIVER/{stream}.gz로
    로그를 gzip 압축해 저장한다. 콘솔 접근 권한이 없어도 이 함수 하나로
    Airflow 태스크 로그에서 바로 원인을 볼 수 있게 한다. 로그 자체를
    못 가져와도(아직 안 올라왔거나 권한 문제) 잡 실패 보고 자체는 막지
    않도록 예외를 삼키고 안내 메시지를 대신 돌려준다."""
    log_path = (
        _EMR_LOGS_DIR
        / "applications"
        / EMR_APPLICATION_ID
        / "jobs"
        / job_run_id
        / "SPARK_DRIVER"
        / f"{stream}.gz"
    )
    try:
        text = gzip.decompress(log_path.read_bytes()).decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - 로그 조회 실패가 잡 실패 처리를 막으면 안 됨
        return f"(드라이버 {stream} 로그를 가져오지 못했습니다: {exc})"

    lines = text.splitlines()
    tail = lines[-_LOG_TAIL_LINES:]
    prefix = f"... (총 {len(lines)}줄 중 마지막 {len(tail)}줄)\n" if len(lines) > len(tail) else ""
    return prefix + "\n".join(tail)


def run_spark_job(
    job_name: str,
    entry_point_script: Path,
    entry_point_args: list[str],
) -> None:
    """EMR Serverless에 Spark 잡을 제출하고 끝날 때까지 기다린다.

    실패(FAILED/CANCELLED)면 예외를 던져 Airflow가 기존 재시도/Slack
    실패 알림 경로를 그대로 타게 한다.
    """
    _validate_rds_env()

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
                "sparkSubmitParameters": (
                    f"--py-files {src_bundle_s3} "
                    f"--conf spark.archives={EMR_PYTHON_ENV_S3_PATH}#environment "
                    # entryPoint 스크립트(드라이버)를 실제로 실행하는 인터프리터는
                    # PYSPARK_DRIVER_PYTHON이 정하고, PYSPARK_PYTHON은 executor에서
                    # UDF 등을 돌릴 때만 쓰인다 - 이걸 안 주면 드라이버가 기본
                    # 시스템 python으로 뜨는 바람에 entryPoint의 최상위 import
                    # (cloudpathlib 등)가 패키징한 venv 없이 실행돼 죽는다.
                    f"--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                    f"--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                    f"--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                    f"{_rds_env_conf()}"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": str(_EMR_LOGS_DIR)},
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
        # EMR Studio 콘솔에 접근할 수 없어도 실패 원인을 바로 볼 수 있게,
        # 드라이버 stdout/stderr 마지막 부분을 Airflow 태스크 로그에 그대로 찍는다.
        logger.error(
            "EMR Serverless 드라이버 stdout(%s):\n%s",
            job_run_id,
            _fetch_driver_log_tail(job_run_id, "stdout"),
        )
        logger.error(
            "EMR Serverless 드라이버 stderr(%s):\n%s",
            job_run_id,
            _fetch_driver_log_tail(job_run_id, "stderr"),
        )
        raise RuntimeError(
            f"EMR Serverless 잡 실패: {job_name} "
            f"(jobRunId={job_run_id}, state={state}, "
            f"detail={job_run.get('stateDetails')})"
        )

    logger.info(f"EMR Serverless 잡 완료: {job_name} (jobRunId={job_run_id})")


def read_json_result(s3_path: str):
    """잡이 저장해둔 JSON 결과 파일을 읽는다."""
    return json.loads(S3Path(s3_path).read_text())
