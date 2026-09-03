#!/bin/sh
# Docker 컨테이너가 예상 못 하게 죽었다 살아나면(RestartCount 증가) Slack으로
# 알린다. 여기에 더해, 감시 대상이 모두 정상일 때만 외부 dead-man heartbeat로
# ping을 보낸다(HEARTBEAT_URL).
#
# restart: unless-stopped 정책은 "죽으면 다시 살리기"만 할 뿐, 죽었다는
# 사실 자체는 아무도 모른다 — 특히 airflow-scheduler/airflow-worker가
# 죽으면 on_failure_callback(Slack 실패 알림)도 그 프로세스 안에서 도는
# 코드라 같이 멈춘다("파이프라인이 통째로 죽었는데 알림도 안 오는"
# 시나리오).
#
# 그런데 이 스크립트 자신도 파이프라인과 같은 호스트의 컨테이너라, 호스트가
# 통째로 죽으면 이 감시도 같이 죽는다. 그래서 매 폴링에서 전 감시 대상이
# 기대 상태(inspect 성공 + running + 헬스체크가 있으면 unhealthy 아님)일
# 때만 HEARTBEAT_URL로 ping을 보낸다 — ping이 grace를 넘겨 끊기면 외부
# dead-man 서비스(healthchecks.io 등)가 "호스트·Docker·감시자 중 하나가
# 죽었다"고 판단해 알린다. 정상일 때만 보내므로, 대상 하나가 죽어도
# ping이 멈춰 같은 경로로 드러난다.
#
# 처음엔 `docker events` 실시간 스트림으로 구현했는데, 로컬 Docker Desktop
# 환경에서 이벤트 스트림 자체가 (필터 없이 테스트해도) 전혀 안 잡히는
# 문제가 있어서 폴링 방식으로 교체했다. 대신 "현재 상태가 running인지"만
# 보지 않고 컨테이너의 누적 RestartCount를 기억해 뒀다가 늘어났는지
# 확인한다 — 그래야 폴링 주기 사이에 죽었다가 이미 자동 복구된 짧은
# 크래시도 놓치지 않는다.
#
# HEARTBEAT_URL 운영 활성화(외부 check 생성·grace 설정·채널 연결) 절차와
# 배포 후 수동 검증은 RELIABILITY_PRINCIPLES.md의 "감시자 dead-man
# heartbeat" 절 참고.
set -eu

# Alpine(docker:27-cli 이미지)에서만 필요한 런타임 의존성. 다른 배포판이나
# 테스트 환경(apk 없음)에서는 이미 설치돼 있다고 보고 건너뛴다.
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache jq curl >/dev/null 2>&1 || true
fi

POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-30}"
STATE_DIR="${STATE_DIR:-/tmp/crash-monitor-state}"
HEARTBEAT_URL="${HEARTBEAT_URL:-}"
HEARTBEAT_TIMEOUT_SECONDS="${HEARTBEAT_TIMEOUT_SECONDS:-10}"
mkdir -p "$STATE_DIR"

echo "[crash-monitor] 폴링 감시 대상: ${WATCH_CONTAINERS:-(없음)}, 주기: ${POLL_INTERVAL_SECONDS}s, heartbeat: ${HEARTBEAT_URL:-(비활성)}"

notify_slack() {
  text="$1"
  if [ -z "${SLACK_WEBHOOK_URL:-}" ]; then
    echo "[crash-monitor] SLACK_WEBHOOK_URL이 없어서 알림을 건너뜁니다"
    return
  fi
  payload=$(jq -n --arg text "$text" '{text: $text}')
  if ! curl -sf -X POST -H "Content-Type: application/json" -d "$payload" "$SLACK_WEBHOOK_URL" >/dev/null; then
    echo "[crash-monitor] Slack 전송 실패"
  fi
}

# dead-man heartbeat. 정상 사이클에서만 호출된다(run_check_cycle이 판단).
# 전송 실패는 삼킨다 — heartbeat 서버 일시 장애로 감시 루프가 멈추면 안
# 된다. ping 한두 번 빠지는 건 외부 서비스의 grace가 흡수한다.
send_heartbeat() {
  [ -n "$HEARTBEAT_URL" ] || return 0
  if curl -sf -o /dev/null --max-time "$HEARTBEAT_TIMEOUT_SECONDS" \
       --retry 2 --retry-delay 1 "$HEARTBEAT_URL"; then
    echo "[crash-monitor] heartbeat 송신 완료"
  else
    echo "[crash-monitor] heartbeat 송신 실패 (무시하고 계속)"
  fi
}

restart_count_of() {
  docker inspect --format '{{.RestartCount}}' "$1" 2>/dev/null || echo "-1"
}

status_of() {
  docker inspect --format '{{.State.Status}}' "$1" 2>/dev/null || echo "존재하지 않음"
}

# 헬스체크가 정의된 컨테이너면 그 상태(healthy/unhealthy/starting), 없으면
# 빈 문자열, inspect 실패면 "?"를 반환한다.
health_of() {
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$1" 2>/dev/null || echo "?"
}

# exit code는 일부러 안 쓴다 — 재시작 정책으로 컨테이너가 다시 running
# 상태가 되면 Docker가 .State.ExitCode를 0으로 되돌려버려서(실측 확인),
# 이 시점(재시작 후 폴링)에 읽으면 항상 "0"이라는 거짓 정보만 주게 된다.
# 반대로 .State.FinishedAt(마지막으로 죽은 시각)은 재시작 후에도 유지돼서
# 이걸로 "언제 죽었는지"는 정확히 알려줄 수 있다 — 그 시각을 알려주면
# 사람이 docker logs --since로 바로 관련 로그를 찾아볼 수 있다.
finished_at_of() {
  docker inspect --format '{{.State.FinishedAt}}' "$1" 2>/dev/null || echo "?"
}

# 한 폴링 사이클: 대상별 재시작 감지 + 전체가 기대 상태면 heartbeat 송신.
run_check_cycle() {
  cycle_any_target=0
  cycle_all_expected=1

  for name in $(echo "${WATCH_CONTAINERS:-}" | tr ',' ' '); do
    cycle_any_target=1
    prev=$(cat "$STATE_DIR/$name" 2>/dev/null || echo "-1")
    current=$(restart_count_of "$name")
    status=$(status_of "$name")
    health=$(health_of "$name")

    if [ "$current" != "$prev" ] && [ "$current" != "-1" ]; then
      finished_at=$(finished_at_of "$name")
      echo "[crash-monitor] ${name}의 RestartCount ${prev} -> ${current} (현재 상태: ${status}, 마지막으로 죽은 시각: ${finished_at})"
      notify_slack ":skull: *컨테이너 재시작 감지*
*이름*: \`${name}\`
*재시작 횟수*: \`${prev} -> ${current}\`
*현재 상태*: \`${status}\`
*마지막으로 죽은 시각*: \`${finished_at}\`
\`restart: unless-stopped\` 정책으로 자동 복구는 됐지만, 왜 죽었는지 확인이 필요합니다.
로그 확인: \`docker logs ${name} --since '${finished_at}'\`"
    fi
    echo "$current" > "$STATE_DIR/$name"

    # heartbeat 자격: inspect 성공(-1 아님) + running + (헬스체크가 있다면)
    # unhealthy 아님. 하나라도 어긋나면 이번 사이클엔 ping을 보내지 않아,
    # "감시자는 살아있지만 대상이 죽었다"가 dead-man 알림으로 드러난다.
    if [ "$current" = "-1" ] || [ "$status" != "running" ] || [ "$health" = "unhealthy" ]; then
      cycle_all_expected=0
    fi
  done

  if [ "$cycle_any_target" = "0" ]; then
    echo "[crash-monitor] WATCH_CONTAINERS가 비어 있어 heartbeat를 보내지 않습니다"
    return 0
  fi
  if [ "$cycle_all_expected" = "1" ]; then
    send_heartbeat
  else
    echo "[crash-monitor] 일부 대상이 기대 상태가 아니라 heartbeat를 건너뜁니다"
  fi
}

# 초기 기준값 저장 — 스크립트 시작 시점의 RestartCount를 기준으로 삼아서,
# 그 이후에 늘어난 것만 새로운 크래시로 취급한다(과거에 이미 있었던
# 재시작 이력까지 매번 재알림하지 않기 위함).
for name in $(echo "${WATCH_CONTAINERS:-}" | tr ',' ' '); do
  restart_count_of "$name" > "$STATE_DIR/$name"
done

# CRASH_MONITOR_ONESHOT=1이면 한 사이클만 돌고 종료한다(테스트용 seam).
if [ "${CRASH_MONITOR_ONESHOT:-0}" = "1" ]; then
  run_check_cycle
  exit 0
fi

while true; do
  sleep "$POLL_INTERVAL_SECONDS"
  run_check_cycle
done
