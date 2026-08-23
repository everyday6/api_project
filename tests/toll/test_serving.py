from unittest.mock import patch

from src.toll.serving import get_toll_values


def test_get_toll_values_returns_values_in_order():
    with patch(
        "src.toll.serving.dynamodb.batch_get_items",
        return_value={
            ("1", "TYPE#4"): {"segment_id": "1", "sk": "TYPE#4", "value": 2.75},
            ("2", "TYPE#4"): {"segment_id": "2", "sk": "TYPE#4", "value": 17.0},
        },
    ) as mock_batch:
        result = get_toll_values(["1", "2"])

    assert result == [2.75, 17.0]
    mock_batch.assert_called_once()


def test_get_toll_values_defaults_missing_segments_to_zero():
    with patch("src.toll.serving.dynamodb.batch_get_items", return_value={}):
        result = get_toll_values(["nonexistent"])

    assert result == [0.0]


def test_get_toll_values_dedupes_before_querying_then_restores_duplicates():
    with patch(
        "src.toll.serving.dynamodb.batch_get_items",
        return_value={("1", "TYPE#4"): {"segment_id": "1", "sk": "TYPE#4", "value": 5.0}},
    ) as mock_batch:
        result = get_toll_values(["1", "1", "1"])

    assert result == [5.0, 5.0, 5.0]
    keys_arg = mock_batch.call_args.args[1]
    assert len(keys_arg) == 1


def test_get_toll_values_falls_back_to_zero_when_dynamodb_unreachable():
    with patch("src.toll.serving.dynamodb.batch_get_items", side_effect=RuntimeError("down")):
        result = get_toll_values(["1", "2", "3"])

    assert result == [0.0, 0.0, 0.0]
