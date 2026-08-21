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
