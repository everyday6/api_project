from unittest.mock import patch

from src.tlc.emr import run_tlc_emr_operation


def test_run_tlc_emr_operation_submits_job_and_reads_result():
    expected = {"passed": [{"filename": "yellow.parquet"}]}

    with patch("src.tlc.emr.run_spark_job") as run_job:
        with patch("src.tlc.emr.read_json_result", return_value=expected) as read_result:
            result = run_tlc_emr_operation(
                "validate_bronze",
                {"bronze_chunk": [{"filename": "yellow.parquet"}]},
            )

    assert result == expected
    kwargs = run_job.call_args.kwargs
    assert kwargs["job_name"].startswith("tlc-validate-bronze-")
    assert kwargs["entry_point_script"].name == "tlc_pipeline_job.py"
    assert kwargs["entry_point_args"][0:2] == ["--operation", "validate_bronze"]
    assert kwargs["entry_point_args"][2] == "--payload-json"
    assert kwargs["max_executors"] is None
    assert read_result.call_args.args[0].endswith(".json")


def test_run_tlc_emr_operation_forwards_max_executors():
    with patch("src.tlc.emr.run_spark_job") as run_job:
        with patch("src.tlc.emr.read_json_result", return_value={}):
            run_tlc_emr_operation("build_silver", {"bronze_chunk": []}, max_executors=3)

    assert run_job.call_args.kwargs["max_executors"] == 3
