from src.common.provenance import formula_version


def test_formula_version_has_label_and_hash():
    version = formula_version("v1", 10)

    label, _, digest = version.partition("+")
    assert label == "v1"
    assert len(digest) == 6
    assert all(c in "0123456789abcdef" for c in digest)


def test_formula_version_is_stable_for_same_inputs():
    assert formula_version("v1", 10, "weighted") == formula_version("v1", 10, "weighted")


def test_formula_version_hash_changes_when_salient_value_changes():
    # 스무딩 윈도우 같은 튜닝값이 바뀌면 라벨을 안 올려도 버전이 달라져야
    # 저장된 값이 코드와 조용히 어긋나지 않는다.
    assert formula_version("v1", 10) != formula_version("v1", 12)


def test_formula_version_label_bump_changes_version():
    assert formula_version("v1", 10) != formula_version("v2", 10)
