from unittest.mock import patch

from src.toll.serving import get_toll_values


def test_get_toll_values_returns_values_in_order():
    with patch(
        "src.toll.serving.rds.batch_get_static_values",
        return_value={
            "1": {"value": 2.75, "collected_date": None, "updated_date": None},
            "2": {"value": 17.0, "collected_date": None, "updated_date": None},
        },
    ) as mock_batch:
        result = get_toll_values(["1", "2"])

    assert result == [2.75, 17.0]
    mock_batch.assert_called_once()


def test_get_toll_values_defaults_missing_segments_to_zero():
    with patch("src.toll.serving.rds.batch_get_static_values", return_value={}):
        result = get_toll_values(["nonexistent"])

    assert result == [0.0]


def test_get_toll_values_dedupes_before_querying_then_restores_duplicates():
    with patch(
        "src.toll.serving.rds.batch_get_static_values",
        return_value={"1": {"value": 5.0, "collected_date": None, "updated_date": None}},
    ) as mock_batch:
        result = get_toll_values(["1", "1", "1"])

    assert result == [5.0, 5.0, 5.0]
    segment_ids_arg = mock_batch.call_args.args[1]
    assert len(segment_ids_arg) == 1


def test_get_toll_values_falls_back_to_zero_when_rds_unreachable():
    with patch("src.toll.serving.rds.batch_get_static_values", side_effect=RuntimeError("down")):
        result = get_toll_values(["1", "2", "3"])

    assert result == [0.0, 0.0, 0.0]
