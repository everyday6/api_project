"""`scripts/docker_crash_monitor.sh`의 heartbeat 계약을 운영 없이 검증한다.

가짜 `docker`/`curl`을 PATH 앞에 놓고, `CRASH_MONITOR_ONESHOT=1`로 한 사이클만
돌린 뒤 가짜 `curl`이 heartbeat URL로 호출됐는지(=송신됐는지)를 본다.

Docker 데몬도 실제 네트워크도 필요 없다. POSIX `sh`가 없는 환경(예: sh가
설치되지 않은 Windows)에서는 전체 모듈을 스킵한다 — CI(Linux 컨테이너)에서
정식으로 돈다.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "docker_crash_monitor.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("sh") is None,
    reason="POSIX sh가 없는 환경에서는 셸 스크립트 테스트를 스킵한다",
)

HEARTBEAT_URL = "http://dead-man.test/ping"

_FAKE_DOCKER = """\
    #!/bin/sh
    # inspect만 흉내낸다. 시나리오는 env로 제어한다.
    [ "$1" = "inspect" ] || { echo "가짜 docker: inspect만 지원" >&2; exit 2; }
    [ "${FAKE_DOCKER_FAIL:-0}" = "1" ] && exit 1
    fmt=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --format) fmt="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    case "$fmt" in
      *RestartCount*) echo "${FAKE_DOCKER_RESTARTCOUNT:-0}" ;;
      *State.Status*) echo "${FAKE_DOCKER_STATUS:-running}" ;;
      *FinishedAt*)   echo "0001-01-01T00:00:00Z" ;;
      *Health*)       echo "${FAKE_DOCKER_HEALTH:-}" ;;
      *)              echo "" ;;
    esac
"""

_FAKE_CURL = """\
    #!/bin/sh
    # 호출 인자를 CURL_LOG에 남기고, FAKE_CURL_FAIL이면 실패한다.
    echo "$*" >> "$CURL_LOG"
    [ "${FAKE_CURL_FAIL:-0}" = "1" ] && exit 22
    exit 0
"""

_FAKE_JQ = "#!/bin/sh\necho '{}'\n"


def _write_exec(path: Path, body: str) -> None:
    # 항상 LF로 쓴다 — Windows 텍스트 모드가 CRLF로 바꾸면 CRLF에 민감한
    # sh 구현에서 스텁이 깨질 수 있다.
    path.write_bytes(textwrap.dedent(body).encode("utf-8"))
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture
def run_monitor(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl_log = tmp_path / "curl.log"
    _write_exec(bin_dir / "docker", _FAKE_DOCKER)
    _write_exec(bin_dir / "curl", _FAKE_CURL)
    _write_exec(bin_dir / "jq", _FAKE_JQ)

    def _run(watch="c1,c2", heartbeat_url=HEARTBEAT_URL, **fake_env):
        env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "CRASH_MONITOR_ONESHOT": "1",
            "STATE_DIR": str(tmp_path / "state"),
            "WATCH_CONTAINERS": watch,
            "CURL_LOG": str(curl_log),
            "POLL_INTERVAL_SECONDS": "1",
        }
        if heartbeat_url is not None:
            env["HEARTBEAT_URL"] = heartbeat_url
        env.update({k: str(v) for k, v in fake_env.items()})
        proc = subprocess.run(
            ["sh", str(SCRIPT)],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        sent = curl_log.read_text(encoding="utf-8") if curl_log.exists() else ""
        return proc, sent

    return _run


def test_heartbeat_sent_when_all_targets_running(run_monitor):
    proc, sent = run_monitor(FAKE_DOCKER_STATUS="running", FAKE_DOCKER_RESTARTCOUNT="0")
    assert proc.returncode == 0, proc.stderr
    assert HEARTBEAT_URL in sent


def test_no_heartbeat_when_docker_inspect_fails(run_monitor):
    proc, sent = run_monitor(FAKE_DOCKER_FAIL="1")
    assert proc.returncode == 0, proc.stderr
    assert sent == ""


def test_no_heartbeat_when_a_target_is_exited(run_monitor):
    proc, sent = run_monitor(FAKE_DOCKER_STATUS="exited")
    assert proc.returncode == 0, proc.stderr
    assert sent == ""


def test_no_heartbeat_when_a_target_is_unhealthy(run_monitor):
    proc, sent = run_monitor(FAKE_DOCKER_STATUS="running", FAKE_DOCKER_HEALTH="unhealthy")
    assert proc.returncode == 0, proc.stderr
    assert sent == ""


def test_no_heartbeat_when_watch_list_empty(run_monitor):
    proc, sent = run_monitor(watch="")
    assert proc.returncode == 0, proc.stderr
    assert sent == ""
    assert "비어 있어" in proc.stdout


def test_no_heartbeat_when_url_not_configured(run_monitor):
    proc, sent = run_monitor(heartbeat_url=None, FAKE_DOCKER_STATUS="running")
    assert proc.returncode == 0, proc.stderr
    assert sent == ""


def test_monitor_survives_heartbeat_http_failure(run_monitor):
    proc, sent = run_monitor(FAKE_CURL_FAIL="1", FAKE_DOCKER_STATUS="running")
    assert proc.returncode == 0, proc.stderr  # 루프가 죽지 않는다
    assert HEARTBEAT_URL in sent  # 시도는 했다
    assert "heartbeat 송신 실패" in proc.stdout
