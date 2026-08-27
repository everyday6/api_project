import json

from src.common import gold_snapshot


def test_write_then_read_round_trip(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    snapshot = {"1": {"avg": 40.0, "spec": 55.0, "exact_value": 30.0, "exact_observed_at": 1755766800.0}}
    gold_snapshot.write_snapshot("type1", snapshot)

    assert gold_snapshot.read_snapshot("type1") == snapshot


def test_read_missing_file_returns_empty_dict(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    assert gold_snapshot.read_snapshot("type1") == {}


def test_read_corrupted_file_returns_empty_dict_instead_of_raising(monkeypatch, tmp_path):
    # "무조건 응답" 원칙상 이 최후의 안전망(폴백의 폴백)에서 예외를 던지면
    # 안 된다 - 손상된 파일이어도 빈 dict로 응답해서 호출부가 다음 단계
    # (코드 상수)로 넘어가게 한다.
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    path = gold_snapshot.snapshot_path("type1")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not valid json {{{")

    assert gold_snapshot.read_snapshot("type1") == {}


def test_write_snapshot_creates_parent_directory(monkeypatch, tmp_path):
    nested_dir = tmp_path / "nested" / "gold_cache"
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", nested_dir)

    gold_snapshot.write_snapshot("type1", {"1": {"avg": 40.0}})

    assert gold_snapshot.snapshot_path("type1").exists()


def test_snapshot_path_is_scoped_by_type_name(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    gold_snapshot.write_snapshot("type1", {"1": {"avg": 1.0}})
    gold_snapshot.write_snapshot("type2", {"1": {"avg": 2.0}})

    assert gold_snapshot.read_snapshot("type1") == {"1": {"avg": 1.0}}
    assert gold_snapshot.read_snapshot("type2") == {"1": {"avg": 2.0}}


def test_write_snapshot_serializes_as_json(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    gold_snapshot.write_snapshot("type1", {"1": {"avg": 40.0}})

    raw = gold_snapshot.snapshot_path("type1").read_text()
    assert json.loads(raw) == {"1": {"avg": 40.0}}
