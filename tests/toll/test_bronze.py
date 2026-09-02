from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from src.toll.bronze import (
    _copy_file_to_bronze,
    upload_cbd_geofence,
    upload_facilities,
    upload_rates,
)


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


def test_upload_rates_rejects_invalid_yaml(tmp_path):
    bad_source = tmp_path / "bad_rates.yaml"
    bad_source.write_text("congestion:\n  - a\n  b: [unterminated\n")

    with pytest.raises(ValueError, match="유효한 YAML"):
        upload_rates(source_path=str(bad_source), bronze_root=tmp_path / "bronze" / "toll")


_VALID_FEATURE_COLLECTION = (
    b'{"type": "FeatureCollection", "features": '
    b'[{"type": "Feature", "properties": {}, "geometry": null}]}'
)


def test_upload_cbd_geofence_downloads_and_validates_json(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"
    fake_response = Mock(content=_VALID_FEATURE_COLLECTION)
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        out_path = upload_cbd_geofence(bronze_root=bronze_root)

    assert out_path.exists()
    assert out_path.read_bytes() == fake_response.content


def test_upload_cbd_geofence_rejects_non_json_response(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"
    fake_response = Mock(content=b"<html>not json</html>")
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        with pytest.raises(ValueError, match="유효한 JSON"):
            upload_cbd_geofence(bronze_root=bronze_root)


def test_upload_cbd_geofence_rejects_non_feature_collection(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"
    fake_response = Mock(content=b'{"error": "not found"}')
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        with pytest.raises(ValueError, match="FeatureCollection"):
            upload_cbd_geofence(bronze_root=bronze_root)


def test_upload_cbd_geofence_rejects_empty_features(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"
    fake_response = Mock(content=b'{"type": "FeatureCollection", "features": []}')
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        with pytest.raises(ValueError, match="features가 비어"):
            upload_cbd_geofence(bronze_root=bronze_root)


def test_upload_cbd_geofence_skips_write_when_content_hash_unchanged(tmp_path):
    bronze_root = tmp_path / "bronze" / "toll"
    fake_response = Mock(content=_VALID_FEATURE_COLLECTION)
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        upload_cbd_geofence(bronze_root=bronze_root)

    with patch("src.toll.bronze.requests.get", return_value=fake_response), \
         patch("src.toll.bronze._copy_file_to_bronze") as mock_copy:
        upload_cbd_geofence(bronze_root=bronze_root)

    # 내용이 그대로면 Bronze 쓰기 자체를 건너뛴다(in-place 덮어쓰기라 무의미).
    mock_copy.assert_not_called()


def test_upload_cbd_geofence_warns_when_content_changes(tmp_path, caplog):
    bronze_root = tmp_path / "bronze" / "toll"

    first = Mock(content=_VALID_FEATURE_COLLECTION)
    first.raise_for_status = Mock()
    with patch("src.toll.bronze.requests.get", return_value=first):
        upload_cbd_geofence(bronze_root=bronze_root)

    changed = Mock(content=(
        b'{"type": "FeatureCollection", "features": '
        b'[{"type": "Feature", "properties": {"NEW": 1}, "geometry": null}]}'
    ))
    changed.raise_for_status = Mock()
    with patch("src.toll.bronze.requests.get", return_value=changed):
        with caplog.at_level("WARNING"):
            upload_cbd_geofence(bronze_root=bronze_root)

    assert any("변경 감지" in r.message for r in caplog.records)


def test_upload_cbd_geofence_does_not_overwrite_existing_file_on_failure(tmp_path):
    # 핵심 회귀 테스트: 새 응답이 검증에 실패하면, Bronze에 이미 있던
    # 정상 파일이 그대로 보존돼야 한다(운영 경로를 먼저 쓰고 나중에
    # 검증하면 이게 깨진다).
    bronze_root = tmp_path / "bronze" / "toll"
    bronze_root.mkdir(parents=True)
    out_path = bronze_root / "cbd_geofence.geojson"
    out_path.write_bytes(_VALID_FEATURE_COLLECTION)

    fake_response = Mock(content=b"<html>server error</html>")
    fake_response.raise_for_status = Mock()

    with patch("src.toll.bronze.requests.get", return_value=fake_response):
        with pytest.raises(ValueError, match="유효한 JSON"):
            upload_cbd_geofence(bronze_root=bronze_root)

    assert out_path.read_bytes() == _VALID_FEATURE_COLLECTION
