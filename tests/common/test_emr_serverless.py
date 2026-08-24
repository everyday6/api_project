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


def test_run_spark_job_raises_on_failure(tmp_script):
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {
        "jobRun": {"state": "FAILED", "stateDetails": "boom"}
    }

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless, "_fetch_driver_log_tail", return_value="mock log"), \
         patch.object(emr_serverless.time, "sleep"):

        with pytest.raises(RuntimeError, match="boom"):
            emr_serverless.run_spark_job("test-job", tmp_script, [])


def test_run_spark_job_configures_s3_log_destination(tmp_script):
    """EMR Studio 콘솔 접근 권한이 없어도 실패 시 드라이버 로그를 직접
    읽어올 수 있도록, 잡 제출 시 s3MonitoringConfiguration을 지정해야 한다."""
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless.time, "sleep"):

        emr_serverless.run_spark_job("test-job", tmp_script, [])

    call_kwargs = mock_client.start_job_run.call_args.kwargs
    log_uri = call_kwargs["configurationOverrides"]["monitoringConfiguration"]["s3MonitoringConfiguration"]["logUri"]
    assert log_uri == str(emr_serverless._EMR_LOGS_DIR)


def test_run_spark_job_logs_driver_output_on_failure(tmp_script):
    """실패 시 드라이버 stdout/stderr 마지막 부분을 Airflow 태스크 로그에
    직접 찍어서, EMR Studio 콘솔 없이도 원인을 바로 볼 수 있어야 한다."""
    mock_client = MagicMock()
    mock_client.start_job_run.return_value = {"jobRunId": "run-1"}
    mock_client.get_job_run.return_value = {
        "jobRun": {"state": "FAILED", "stateDetails": "boom"}
    }

    with patch.object(emr_serverless, "_upload_src_bundle", return_value="s3://bucket/src.zip"), \
         patch.object(emr_serverless, "_upload_script", return_value="s3://bucket/job.py"), \
         patch.object(emr_serverless.boto3, "client", return_value=mock_client), \
         patch.object(emr_serverless, "_fetch_driver_log_tail", return_value="the actual traceback") as mock_fetch, \
         patch.object(emr_serverless.time, "sleep"), \
         patch.object(emr_serverless.logger, "error") as mock_log_error:

        with pytest.raises(RuntimeError):
            emr_serverless.run_spark_job("test-job", tmp_script, [])

    mock_fetch.assert_any_call("run-1", "stdout")
    mock_fetch.assert_any_call("run-1", "stderr")
    logged_text = "\n".join(str(call.args) for call in mock_log_error.call_args_list)
    assert "the actual traceback" in logged_text
