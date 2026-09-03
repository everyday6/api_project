import json
from unittest.mock import patch

from src.common import gold_snapshot
from src.common.gold_snapshot import LazySnapshot


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


# --- read_snapshot_result: hit / miss 분류 ---

def test_read_snapshot_result_hit(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {"1": {"avg": 40.0}})

    assert gold_snapshot.read_snapshot_result("type1") == ({"1": {"avg": 40.0}}, "hit")


def test_read_snapshot_result_missing_is_miss(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    assert gold_snapshot.read_snapshot_result("type1") == ({}, "miss")


def test_read_snapshot_result_corrupted_is_miss(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    path = gold_snapshot.snapshot_path("type1")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not valid json {{{")

    assert gold_snapshot.read_snapshot_result("type1") == ({}, "miss")


# --- LazySnapshot: 실패를 프로세스 수명 내내 고정하지 않는다 ---

def test_lazy_snapshot_caches_hit_and_does_not_reread():
    snap = LazySnapshot("type1")
    with patch.object(gold_snapshot, "read_snapshot_result", return_value=({"1": {"avg": 1.0}}, "hit")) as mock_read:
        assert snap.get() == {"1": {"avg": 1.0}}
        assert snap.get() == {"1": {"avg": 1.0}}
        assert snap.get() == {"1": {"avg": 1.0}}

    mock_read.assert_called_once()


def test_lazy_snapshot_retries_after_transient_failure(monkeypatch):
    # 예전엔 첫 로드가 {}면 그 빈 결과를 프로세스 수명 내내 고정했다 -
    # 이제는 miss면 backoff 뒤 다시 시도한다. backoff를 0으로 만들어 확인.
    monkeypatch.setattr(gold_snapshot, "_RETRY_BACKOFF_SECONDS", 0)
    snap = LazySnapshot("type1")

    with patch.object(
        gold_snapshot, "read_snapshot_result",
        side_effect=[({}, "miss"), ({"1": {"avg": 2.0}}, "hit")],
    ) as mock_read:
        assert snap.get() == {}                    # 첫 시도 miss -> 빈 dict
        assert snap.get() == {"1": {"avg": 2.0}}   # 재시도 hit

    assert mock_read.call_count == 2


def test_lazy_snapshot_first_read_happens_even_when_monotonic_clock_is_small():
    # time.monotonic()은 부팅 후 경과 초 - 갓 뜬 Lambda에서는 backoff 창(30초)
    # 보다 작을 수 있다. "아직 실패 안 함"을 0.0으로 취급하면 첫 get()이
    # backoff에 걸려 S3를 아예 안 읽는다(RDS도 죽어있으면 스냅샷 폴백 계층이
    # 통째로 스킵). 첫 읽기는 monotonic 값과 무관하게 일어나야 한다.
    snap = LazySnapshot("type1")
    with patch.object(gold_snapshot.time, "monotonic", return_value=5.0):
        with patch.object(
            gold_snapshot, "read_snapshot_result", return_value=({"1": {"avg": 1.0}}, "hit")
        ) as mock_read:
            assert snap.get() == {"1": {"avg": 1.0}}

    mock_read.assert_called_once()


def test_lazy_snapshot_backoff_avoids_rereading_after_miss():
    # backoff 창(기본 30초) 안에서는 S3를 다시 두드리지 않고 현재 캐시({})를 준다.
    snap = LazySnapshot("type1")
    with patch.object(gold_snapshot, "read_snapshot_result", return_value=({}, "miss")) as mock_read:
        assert snap.get() == {}
        assert snap.get() == {}
        assert snap.get() == {}

    mock_read.assert_called_once()
