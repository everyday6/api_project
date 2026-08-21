import pandas as pd

from src.lion.gold2 import _compute_is_routable


def test_is_routable_true_for_normal_street():
    df = pd.DataFrame([{"RW_TYPE": "1", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert result.iloc[0] is True or bool(result.iloc[0]) is True


def test_is_routable_false_for_non_routable_rw_type():
    # RW_TYPE=6 (Path/Trail) -> 차량 통행 불가
    df = pd.DataFrame([{"RW_TYPE": "6", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False


def test_is_routable_false_for_non_physical_feature_type():
    # FeatureTyp != "0" -> 비물리적 세그먼트(경계선 등)
    df = pd.DataFrame([{"RW_TYPE": "1", "FeatureTyp": "5"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False


def test_is_routable_false_for_missing_rw_type():
    df = pd.DataFrame([{"RW_TYPE": "", "FeatureTyp": "0"}])

    result = _compute_is_routable(df)

    assert bool(result.iloc[0]) is False
