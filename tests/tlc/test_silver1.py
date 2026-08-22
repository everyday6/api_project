from datetime import datetime

from src.common.config import TAXI_TYPES
from src.common.downloader import build_filename
from src.tlc.silver1 import _find_pending_silver_files


def test_find_pending_silver_files_recovers_bronze_without_successful_silver(tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    bronze_root.mkdir()
    service_month = datetime(2026, 5, 1)

    completed_type = TAXI_TYPES[0]
    pending_type = TAXI_TYPES[1]
    for taxi_type in (completed_type, pending_type):
        filename = build_filename(taxi_type, 2026, 5)
        (bronze_root / filename).touch()

    completed_silver = silver_root / build_filename(completed_type, 2026, 5).removesuffix(
        ".parquet"
    )
    completed_silver.mkdir(parents=True)
    (completed_silver / "_SUCCESS").touch()

    pending = _find_pending_silver_files(
        [service_month],
        bronze_root=bronze_root,
        silver_root=silver_root,
    )

    assert pending == [{
        "taxi_type": pending_type,
        "filename": build_filename(pending_type, 2026, 5),
        "bronze_path": str(bronze_root / build_filename(pending_type, 2026, 5)),
    }]
