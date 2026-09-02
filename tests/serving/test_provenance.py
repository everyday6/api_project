from src.serving import provenance as prov


def test_to_provenance_splits_token_into_two_axes():
    tokens = ["rds:observed", "s3_snapshot:historical_average", "code:static_default"]

    assert prov.to_provenance(tokens) == [
        {"storage_source": "rds", "value_basis": "observed"},
        {"storage_source": "s3_snapshot", "value_basis": "historical_average"},
        {"storage_source": "code", "value_basis": "static_default"},
    ]


def test_to_provenance_defaults_missing_basis_to_static_default():
    assert prov.to_provenance(["code"]) == [
        {"storage_source": "code", "value_basis": "static_default"},
    ]


def test_legacy_sources_type1_projects_by_value_basis_regardless_of_storage():
    provenance = [
        {"storage_source": "rds", "value_basis": "observed"},
        {"storage_source": "memory_cache", "value_basis": "observed"},
        {"storage_source": "s3_snapshot", "value_basis": "historical_average"},
        {"storage_source": "code", "value_basis": "static_default"},
    ]
    assert prov.legacy_sources(1, provenance) == ["fresh", "fresh", "avg", "hardcoded"]


def test_legacy_sources_type2_keeps_global_and_snapshot_distinct():
    provenance = [
        {"storage_source": "rds", "value_basis": "segment_value"},
        {"storage_source": "rds", "value_basis": "global_default"},
        {"storage_source": "s3_snapshot", "value_basis": "segment_value"},
        {"storage_source": "s3_snapshot", "value_basis": "global_default"},
        {"storage_source": "code", "value_basis": "static_default"},
    ]
    assert prov.legacy_sources(2, provenance) == [
        "rds", "global", "snapshot", "hardcoded", "hardcoded",
    ]


def test_legacy_sources_type4_inferred_zero_still_projects_to_rds():
    # 하위 호환: 예전 sources는 "읽은 0"과 "추론한 0"을 구분 못 했으므로
    # 둘 다 rds로 유지된다(새 정보는 provenance에만).
    provenance = [
        {"storage_source": "rds", "value_basis": "segment_value"},
        {"storage_source": "rds", "value_basis": "implicit_zero"},
        {"storage_source": "code", "value_basis": "implicit_zero"},
    ]
    assert prov.legacy_sources(4, provenance) == ["rds", "rds", "hardcoded"]


def test_legacy_source_unknown_combo_falls_back_to_hardcoded():
    assert prov.legacy_source(1, {"storage_source": "code", "value_basis": "static_default"}) == "hardcoded"
    assert prov.legacy_source(3, {"storage_source": "code", "value_basis": "static_default"}) == "hardcoded"
