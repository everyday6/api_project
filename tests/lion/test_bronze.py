import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.lion.bronze import ingest_lion


def _fake_zip_bytes() -> bytes:
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("LION.gdb/a00000001.gdbtable", b"content")
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


def test_ingest_lion_downloads_and_uploads_when_no_previous_etag(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("abc123")) as mock_head, \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(_fake_zip_bytes())) as mock_get:
        result = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_head.assert_called_once()
    assert mock_head.call_args.kwargs.get("allow_redirects") is True
    mock_get.assert_called_once()
    assert result["changed"] is True
    dest_dir = Path(result["path"])
    assert dest_dir.exists()
    assert (dest_dir / "_metadata.txt").read_text().count("_etag=abc123") == 1
    assert (bronze_root / "_latest_etag.txt").read_text().count("_etag=abc123") == 1


def test_ingest_lion_skips_download_when_etag_unchanged(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=abc123\n")

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("abc123")), \
         patch("src.lion.bronze.requests.get") as mock_get:
        result = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_get.assert_not_called()
    assert result == {"path": None, "changed": False}


def test_ingest_lion_redownloads_when_etag_changed(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=old-etag\n")

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(_fake_zip_bytes())) as mock_get:
        result = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_get.assert_called_once()
    assert result["changed"] is True
    assert (bronze_root / "_latest_etag.txt").read_text().count("_etag=new-etag") == 1


def test_ingest_lion_raises_when_zip_has_no_gdb(tmp_path):
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", b"no gdb here")

    bronze_root = tmp_path / "bronze" / "lion"

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("abc123")), \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(buf.getvalue())):
        with pytest.raises(RuntimeError, match="유효한 .gdb"):
            ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)
