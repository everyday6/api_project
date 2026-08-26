import io
import zipfile
from unittest.mock import Mock, patch

from src.taxi_zone.bronze import ingest_taxi_zone_shapefile, mark_taxi_zone_etag


def _fake_shapefile_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("taxi_zones/taxi_zones.shp", b"shp")
        zf.writestr("taxi_zones/taxi_zones.dbf", b"dbf")
        zf.writestr("taxi_zones/taxi_zones.shx", b"shx")
    return buf.getvalue()


def _mock_head(etag: str | None):
    resp = Mock()
    resp.raise_for_status = Mock()
    resp.headers = {"ETag": f'"{etag}"'} if etag else {}
    return resp


def _mock_get(content: bytes):
    resp = Mock(content=content)
    resp.raise_for_status = Mock()
    return resp


def test_ingest_taxi_zone_downloads_when_no_previous_etag(tmp_path):
    bronze_root = tmp_path / "bronze" / "taxi_zone"

    with patch("src.taxi_zone.bronze.requests.head", return_value=_mock_head("abc123")), \
         patch("src.taxi_zone.bronze.requests.get", return_value=_mock_get(_fake_shapefile_zip_bytes())) as mock_get:
        result = ingest_taxi_zone_shapefile(bronze_root=bronze_root)

    mock_get.assert_called_once()
    assert result["changed"] is True
    assert result["etag"] == "abc123"
    # ingest 자체는 ETag 마커를 안 쓴다 - Silver1 build까지 성공한 뒤에
    # mark_taxi_zone_etag가 별도로 쓴다.
    assert not (bronze_root / "_latest_etag.txt").exists()


def test_ingest_taxi_zone_skips_download_when_etag_unchanged(tmp_path):
    bronze_root = tmp_path / "bronze" / "taxi_zone"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=abc123\n")

    with patch("src.taxi_zone.bronze.requests.head", return_value=_mock_head("abc123")), \
         patch("src.taxi_zone.bronze.requests.get") as mock_get:
        result = ingest_taxi_zone_shapefile(bronze_root=bronze_root)

    mock_get.assert_not_called()
    assert result["changed"] is False
    assert result["etag"] == "abc123"


def test_ingest_taxi_zone_retries_automatically_after_downstream_failure(tmp_path):
    """핵심 회귀 테스트: Bronze는 성공했는데 그 뒤 Silver1(build)이 실패해서
    mark_taxi_zone_etag가 한 번도 안 불린 상황을 재현한다 - 다음 스케줄
    실행이 "원본 그대로"로 오판하지 않고 자동으로 다시 시도해야 한다."""
    bronze_root = tmp_path / "bronze" / "taxi_zone"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=old-etag\n")

    with patch("src.taxi_zone.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.taxi_zone.bronze.requests.get", return_value=_mock_get(_fake_shapefile_zip_bytes())):
        first = ingest_taxi_zone_shapefile(bronze_root=bronze_root)
    assert first["changed"] is True
    # mark_taxi_zone_etag를 일부러 호출하지 않는다 - Silver1 build 실패를 흉내.

    with patch("src.taxi_zone.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.taxi_zone.bronze.requests.get", return_value=_mock_get(_fake_shapefile_zip_bytes())) as mock_get_2:
        second = ingest_taxi_zone_shapefile(bronze_root=bronze_root)

    mock_get_2.assert_called_once()
    assert second["changed"] is True


def test_mark_taxi_zone_etag_writes_marker(tmp_path):
    bronze_root = tmp_path / "bronze" / "taxi_zone"
    bronze_root.mkdir(parents=True)

    mark_taxi_zone_etag({"changed": True, "etag": "new-etag"}, bronze_root=bronze_root)

    assert (bronze_root / "_latest_etag.txt").read_text().count("_etag=new-etag") == 1


def test_mark_taxi_zone_etag_noop_when_no_etag(tmp_path):
    bronze_root = tmp_path / "bronze" / "taxi_zone"
    bronze_root.mkdir(parents=True)

    mark_taxi_zone_etag({"changed": False}, bronze_root=bronze_root)

    assert not (bronze_root / "_latest_etag.txt").exists()
