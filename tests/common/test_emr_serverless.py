from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.common import emr_serverless


@pytest.fixture
def tmp_script(tmp_path):
    script = tmp_path / "job.py"
    script.write_text("print('hello')")
    return script


def test_run_spark_job_success(tmp_script):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        emr_serverless.run_spark_job("test-job", tmp_script, ["--foo", "bar"])

    mock_client.start_job_run.assert_called_once()
    call_kwargs = mock_client.start_job_run.call_args.kwargs
    assert call_kwargs["jobDriver"]["sparkSubmit"]["entryPointArguments"] == ["--foo", "bar"]

    spark_submit_parameters = call_kwargs["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]
    assert "--py-files s3://bucket/src.zip" in spark_submit_parameters


def test_run_spark_job_packages_python_env_via_spark_archives(tmp_script):
    """서드파티 의존성(pandas/geopandas/shapely 등)이 EMR Serverless 기본
    이미지에 없어 ModuleNotFoundError로 죽는 문제(C3)를 막기 위해, venv-pack으로
    패키징한 파이썬 환경을 spark.archives로 실어서 드라이버/executor 둘 다
    그 환경의 python을 쓰도록 강제해야 한다."""
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        emr_serverless.run_spark_job("test-job", tmp_script, ["--foo", "bar"])

    call_kwargs = mock_client.start_job_run.call_args.kwargs
    spark_submit_parameters = call_kwargs["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]

    assert f"spark.archives={emr_serverless.EMR_PYTHON_ENV_S3_PATH}#environment" in spark_submit_parameters
    assert "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python" in spark_submit_parameters
    assert "spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python" in spark_submit_parameters
    assert "spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python" in spark_submit_parameters


def test_run_spark_job_caps_worker_resources(tmp_script):
    """dynamic allocation을 켜둔 채 executor 상한을 안 주면 Spark가 데이터량을
    보고 자체적으로 executor를 늘려서, 여러 파이프라인 잡이 겹칠 때 계정의
    동시 사용 vCPU 쿼터를 넘겨버린다(ServiceQuotaExceededException으로 실제
    겪음 - segment_time_pipeline의 submit_nav_time_job이 이걸로 죽었었다).
    잡마다 작은 고정 리소스로 상한을 걸어 이 상황을 막는다."""
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        emr_serverless.run_spark_job("test-job", tmp_script, ["--foo", "bar"])

    call_kwargs = mock_client.start_job_run.call_args.kwargs
    spark_submit_parameters = call_kwargs["jobDriver"]["sparkSubmit"]["sparkSubmitParameters"]

    assert "spark.dynamicAllocation.enabled=false" in spark_submit_parameters
    assert "spark.executor.instances=2" in spark_submit_parameters
    assert "spark.executor.cores=1" in spark_submit_parameters
    assert "spark.executor.memory=2g" in spark_submit_parameters
    assert "spark.driver.cores=1" in spark_submit_parameters
    assert "spark.driver.memory=2g" in spark_submit_parameters


def test_run_spark_job_raises_on_failure(tmp_script):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {
        "jobRun": {"state": "FAILED", "stateDetails": "boom"}
    }

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        with pytest.raises(RuntimeError, match="boom"):
            emr_serverless.run_spark_job("test-job", tmp_script, [])
