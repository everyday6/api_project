import zipfile

import pandas as pd
import pytest

from src.common import file_validation as fv


# ---------------------------------------------------------------------------
# validate_non_empty
# ---------------------------------------------------------------------------

def test_validate_non_empty_passes_for_real_file(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("hello")

    fv.validate_non_empty(path)  # 예외 없이 통과해야 한다.


def test_validate_non_empty_raises_when_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        fv.validate_non_empty(tmp_path / "nope.txt")


def test_validate_non_empty_raises_when_zero_bytes(tmp_path):
    path = tmp_path / "empty.txt"
    path.write_bytes(b"")

    with pytest.raises(ValueError, match="빈 파일"):
        fv.validate_non_empty(path)


def test_validate_non_empty_raises_when_path_is_a_directory(tmp_path):
    directory = tmp_path / "some_dir"
    directory.mkdir()

    with pytest.raises(ValueError, match="디렉터리"):
        fv.validate_non_empty(directory)


# ---------------------------------------------------------------------------
# validate_parquet
# ---------------------------------------------------------------------------

def test_validate_parquet_passes_for_valid_file(tmp_path):
    path = tmp_path / "data.parquet"
    pd.DataFrame([{"segment_id": "1", "value": 30}]).to_parquet(path, index=False)

    fv.validate_parquet(path)  # 예외 없이 통과해야 한다.


def test_validate_parquet_raises_for_non_parquet_content(tmp_path):
    path = tmp_path / "fake.parquet"
    path.write_text("this is not parquet")

    with pytest.raises(ValueError, match="유효한 Parquet"):
        fv.validate_parquet(path)


def test_validate_parquet_checks_required_columns(tmp_path):
    path = tmp_path / "data.parquet"
    pd.DataFrame([{"segment_id": "1"}]).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="필수 컬럼"):
        fv.validate_parquet(path, required_columns=["segment_id", "value"])


def test_validate_parquet_passes_when_required_columns_present(tmp_path):
    path = tmp_path / "data.parquet"
    pd.DataFrame([{"segment_id": "1", "value": 30}]).to_parquet(path, index=False)

    fv.validate_parquet(path, required_columns=["segment_id", "value"])


# ---------------------------------------------------------------------------
# validate_zip
# ---------------------------------------------------------------------------

def test_validate_zip_passes_for_valid_zip(tmp_path):
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("LION.gdb/a00000001.gdbtable", b"content")

    fv.validate_zip(path)  # 예외 없이 통과해야 한다.


def test_validate_zip_raises_for_non_zip_content(tmp_path):
    path = tmp_path / "fake.zip"
    path.write_text("this is not a zip")

    with pytest.raises(ValueError, match="유효한 ZIP"):
        fv.validate_zip(path)


def test_validate_zip_checks_required_files_present(tmp_path):
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("LION.gdb/a00000001.gdbtable", b"content")

    fv.validate_zip(path, required_files=["*.gdb/*"])  # 통과해야 한다.


def test_validate_zip_raises_when_required_files_missing(tmp_path):
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("readme.txt", b"content")

    with pytest.raises(ValueError, match="필요한 파일이 없습니다"):
        fv.validate_zip(path, required_files=["*.gdb/*"])


def test_validate_zip_skips_crc_check_by_default(tmp_path):
    # deep_check 기본값(False)이면 testzip()을 안 불러서 손상된 CRC도
    # 못 잡는다 - 대용량 ZIP에서 이중으로 다 읽는 비용을 피하기 위한
    # 의도된 트레이드오프다. 여기서는 "기본값으론 통과한다"만 확인한다.
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("readme.txt", b"content")

    fv.validate_zip(path)  # deep_check=False(기본) - 예외 없이 통과.


def test_validate_zip_deep_check_detects_crc_corruption(tmp_path):
    path = tmp_path / "archive.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("readme.txt", b"content")

    # ZIP 컨테이너 자체는 정상이되 내부 데이터를 손상시켜 CRC 불일치를
    # 만든다 - 파일 뒷부분(압축 데이터 영역) 바이트를 하나 뒤집는다.
    raw = bytearray(path.read_bytes())
    raw[-10] ^= 0xFF
    path.write_bytes(bytes(raw))

    with pytest.raises((ValueError, zipfile.BadZipFile)):
        fv.validate_zip(path, deep_check=True)


# ---------------------------------------------------------------------------
# validate_yaml / validate_json
# ---------------------------------------------------------------------------

def test_validate_yaml_passes_for_valid_yaml(tmp_path):
    path = tmp_path / "rates.yaml"
    path.write_text("congestion:\n  taxi_flat_rate: 0.75\n")

    fv.validate_yaml(path)  # 예외 없이 통과해야 한다.


def test_validate_yaml_raises_for_invalid_yaml(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("congestion:\n  - a\n  b: [unterminated\n")

    with pytest.raises(ValueError, match="유효한 YAML"):
        fv.validate_yaml(path)


def test_validate_yaml_raises_for_empty_file(tmp_path):
    # yaml.safe_load("")는 예외가 아니라 None을 반환한다(빈 문서도
    # 문법적으로는 유효한 YAML) - validate_non_empty()로 먼저 걸러야
    # 이 케이스를 통과시키지 않는다.
    path = tmp_path / "empty.yaml"
    path.write_bytes(b"")

    with pytest.raises(ValueError, match="빈 파일"):
        fv.validate_yaml(path)


def test_validate_json_passes_for_valid_json(tmp_path):
    path = tmp_path / "data.json"
    path.write_text('{"type": "FeatureCollection", "features": []}')

    fv.validate_json(path)  # 예외 없이 통과해야 한다.


def test_validate_json_raises_for_invalid_json(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not valid json")

    with pytest.raises(ValueError, match="유효한 JSON"):
        fv.validate_json(path)
