import zipfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.lion.bronze import ingest_lion, mark_lion_etag


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
    assert result["etag"] == "abc123"
    dest_dir = Path(result["path"])
    assert dest_dir.exists()
    assert (dest_dir / "_metadata.txt").read_text().count("_etag=abc123") == 1
    # ingest_lion 자체는 ETag 마커를 안 쓴다 - Silver1 publish까지 성공한
    # 뒤에 mark_lion_etag가 별도로 쓴다(아래 test_mark_lion_etag_* 참고).
    assert not (bronze_root / "_latest_etag.txt").exists()


def test_ingest_lion_skips_download_when_etag_unchanged(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=abc123\n")

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("abc123")), \
         patch("src.lion.bronze.requests.get") as mock_get:
        result = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_get.assert_not_called()
    assert result == {"path": None, "changed": False, "etag": "abc123"}


def test_ingest_lion_redownloads_when_etag_changed(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=old-etag\n")

    with patch("src.lion.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(_fake_zip_bytes())) as mock_get:
        result = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_get.assert_called_once()
    assert result["changed"] is True
    assert result["etag"] == "new-etag"
    # 마커는 아직 그대로 old-etag를 가리켜야 한다 - mark_lion_etag가
    # publish 성공 후 별도로 호출돼야 갱신된다.
    assert (bronze_root / "_latest_etag.txt").read_text().count("_etag=old-etag") == 1


def test_ingest_lion_retries_automatically_after_downstream_failure(tmp_path):
    """핵심 회귀 테스트: Bronze는 성공했는데 그 뒤 Silver1이 실패해서
    mark_lion_etag가 한 번도 안 불린 상황을 재현한다 - 다음 스케줄
    실행이 "원본 그대로"로 오판하지 않고 자동으로 다시 시도해야 한다."""
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)
    (bronze_root / "_latest_etag.txt").write_text("_etag=old-etag\n")

    # 1회차: Bronze 다운로드는 성공하지만(mark_lion_etag는 호출 안 함 -
    # Silver1이 실패했다고 가정), 마커는 여전히 old-etag.
    with patch("src.lion.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(_fake_zip_bytes())):
        first = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)
    assert first["changed"] is True

    # 2회차(다음 스케줄 실행): 사람이 수동으로 아무것도 안 건드렸어도,
    # 마커가 여전히 old-etag를 가리키므로 "원본이 바뀐 것"으로 다시
    # 판단해 자동으로 재다운로드해야 한다.
    with patch("src.lion.bronze.requests.head", return_value=_mock_head("new-etag")), \
         patch("src.lion.bronze.requests.get", return_value=_mock_get(_fake_zip_bytes())) as mock_get_2:
        second = ingest_lion(version_date="2026-04-01", bronze_root=bronze_root)

    mock_get_2.assert_called_once()
    assert second["changed"] is True


def test_mark_lion_etag_writes_marker(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)

    mark_lion_etag({"changed": True, "etag": "new-etag"}, bronze_root=bronze_root)

    assert (bronze_root / "_latest_etag.txt").read_text().count("_etag=new-etag") == 1


def test_mark_lion_etag_noop_when_no_etag(tmp_path):
    bronze_root = tmp_path / "bronze" / "lion"
    bronze_root.mkdir(parents=True)

    mark_lion_etag({"changed": False}, bronze_root=bronze_root)

    assert not (bronze_root / "_latest_etag.txt").exists()


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
