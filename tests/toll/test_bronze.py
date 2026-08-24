from pathlib import Path
from unittest.mock import Mock

import yaml

from src.toll.bronze import _copy_file_to_bronze, upload_facilities, upload_rates


def test_upload_rates_copies_yaml_to_bronze(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"

    out_path = upload_rates(
        source_path="config/toll_rates.yaml",
        bronze_root=bronze_root,
    )

    assert out_path.exists()
    data = yaml.safe_load(out_path.read_text())
    assert data["congestion"]["taxi_flat_rate"] == 0.75
    assert "queens_midtown_tunnel" in data["road"]


def test_upload_facilities_copies_yaml_to_bronze(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"

    out_path = upload_facilities(
        source_path="config/toll_facilities.yaml",
        bronze_root=bronze_root,
    )

    assert out_path.exists()
    data = yaml.safe_load(out_path.read_text())
    assert data["lincoln_tunnel"]["street_contains"] == "LINCOLN TUNNEL"


def test_copy_file_to_bronze_uses_upload_for_remote_path(tmp_path):
    source = tmp_path / "source.yaml"
    source.write_text("value: 1")
    remote_path = Mock()

    _copy_file_to_bronze(str(source), remote_path)

    remote_path.upload_from.assert_called_once_with(Path(source), force_overwrite_to_cloud=True)
