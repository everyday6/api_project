from unittest.mock import MagicMock, patch

from src.lion import bronze


def test_check_new_lion_release_true_when_no_marker(tmp_path):
    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is True


def test_check_new_lion_release_false_when_unchanged(tmp_path):
    marker_path = tmp_path / "_last_checked_last_modified.txt"
    marker_path.write_text("Wed, 01 Jan 2026 00:00:00 GMT")

    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Wed, 01 Jan 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is False


def test_check_new_lion_release_true_when_changed_and_updates_marker(tmp_path):
    marker_path = tmp_path / "_last_checked_last_modified.txt"
    marker_path.write_text("Wed, 01 Jan 2026 00:00:00 GMT")

    with patch.object(bronze.requests, "head") as mock_head:
        mock_head.return_value = MagicMock(
            status_code=200, headers={"Last-Modified": "Thu, 02 Apr 2026 00:00:00 GMT"}
        )
        result = bronze.check_new_lion_release(marker_dir=tmp_path)

    assert result is True
    assert marker_path.read_text() == "Thu, 02 Apr 2026 00:00:00 GMT"
