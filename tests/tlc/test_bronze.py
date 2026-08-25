import shutil
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.tlc import bronze


def _write_parquet(path, rows):
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_store_bronze_uploads_valid_parquet(tmp_path, monkeypatch):
    # 로컬 Path는 upload_from()이 없다(S3Path 전용 메서드) - 로컬 파일
    # 복사로 흉내내는 걸 임시로 붙여서 실제 업로드 없이 이 경로를 검증한다.
    monkeypatch.setattr(Path, "upload_from", lambda self, src: shutil.copyfile(src, self), raising=False)

    tmp_file = tmp_path / "yellow_tripdata_2026-08.parquet"
    _write_parquet(tmp_file, [{"segment_id": "1"}])
    bronze_root = tmp_path / "bronze"

    with patch.object(bronze, "BRONZE_ROOT", bronze_root):
        result = bronze.store_bronze.function({
            "taxi_type": "yellow",
            "filename": "yellow_tripdata_2026-08.parquet",
            "tmp_path": str(tmp_file),
        })

    assert result["bronze_path"] == str(bronze_root / "yellow_tripdata_2026-08.parquet")
    assert (bronze_root / "yellow_tripdata_2026-08.parquet").exists()
    assert not tmp_file.exists()  # 업로드 성공 후 tmp 파일은 삭제됨


def test_store_bronze_rejects_corrupt_parquet_and_does_not_upload(tmp_path):
    tmp_file = tmp_path / "yellow_tripdata_2026-08.parquet"
    tmp_file.write_text("this is not a real parquet file")
    bronze_root = tmp_path / "bronze"

    with patch.object(bronze, "BRONZE_ROOT", bronze_root):
        with pytest.raises(ValueError, match="유효한 Parquet"):
            bronze.store_bronze.function({
                "taxi_type": "yellow",
                "filename": "yellow_tripdata_2026-08.parquet",
                "tmp_path": str(tmp_file),
            })

    # 검증에 실패했으니 Bronze엔 아무것도 올라가면 안 된다.
    assert not (bronze_root / "yellow_tripdata_2026-08.parquet").exists()


def test_store_bronze_skips_when_already_in_bronze(tmp_path):
    bronze_root = tmp_path / "bronze"
    bronze_root.mkdir(parents=True)
    existing = bronze_root / "yellow_tripdata_2026-08.parquet"
    _write_parquet(existing, [{"segment_id": "old"}])

    tmp_file = tmp_path / "yellow_tripdata_2026-08.parquet.tmp"
    tmp_file.write_text("garbage - should never be validated since dedup skips first")

    with patch.object(bronze, "BRONZE_ROOT", bronze_root):
        result = bronze.store_bronze.function({
            "taxi_type": "yellow",
            "filename": "yellow_tripdata_2026-08.parquet",
            "tmp_path": str(tmp_file),
        })

    assert result["bronze_path"] == str(existing)
    assert not tmp_file.exists()  # 중복이라 임시 파일은 그냥 삭제됨
