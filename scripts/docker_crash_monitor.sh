#!/bin/sh
# Docker 컨테이너가 예상 못 하게 죽었다 살아나면(RestartCount 증가) Slack으로
# 알린다.
#
# restart: unless-stopped 정책은 "죽으면 다시 살리기"만 할 뿐, 죽었다는
# 사실 자체는 아무도 모른다 — 특히 airflow-scheduler/airflow-worker가
# 죽으면 on_failure_callback(Slack 실패 알림)도 그 프로세스 안에서 도는
# 코드라 같이 멈춘다("파이프라인이 통째로 죽었는데 알림도 안 오는"
# 시나리오).
#
# 처음엔 `docker events` 실시간 스트림으로 구현했는데, 로컬 Docker Desktop
# 환경에서 이벤트 스트림 자체가 (필터 없이 테스트해도) 전혀 안 잡히는
# 문제가 있어서 폴링 방식으로 교체했다. 대신 "현재 상태가 running인지"만
# 보지 않고 컨테이너의 누적 RestartCount를 기억해 뒀다가 늘어났는지
# 확인한다 — 그래야 폴링 주기 사이에 죽었다가 이미 자동 복구된 짧은
# 크래시도 놓치지 않는다.
set -eu

apk add --no-cache jq curl >/dev/null 2>&1

POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-30}"
STATE_DIR=/tmp/crash-monitor-state
mkdir -p "$STATE_DIR"

echo "[crash-monitor] 폴링 감시 대상: ${WATCH_CONTAINERS:-(없음)}, 주기: ${POLL_INTERVAL_SECONDS}s"

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

restart_count_of() {
  docker inspect --format '{{.RestartCount}}' "$1" 2>/dev/null || echo "-1"
}

status_of() {
  docker inspect --format '{{.State.Status}}' "$1" 2>/dev/null || echo "존재하지 않음"
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

# 초기 기준값 저장 — 스크립트 시작 시점의 RestartCount를 기준으로 삼아서,
# 그 이후에 늘어난 것만 새로운 크래시로 취급한다(과거에 이미 있었던
# 재시작 이력까지 매번 재알림하지 않기 위함).
for name in $(echo "${WATCH_CONTAINERS:-}" | tr ',' ' '); do
  restart_count_of "$name" > "$STATE_DIR/$name"
done

while true; do
  sleep "$POLL_INTERVAL_SECONDS"
  for name in $(echo "${WATCH_CONTAINERS:-}" | tr ',' ' '); do
    prev=$(cat "$STATE_DIR/$name" 2>/dev/null || echo "-1")
    current=$(restart_count_of "$name")

    if [ "$current" != "$prev" ] && [ "$current" != "-1" ]; then
      status=$(status_of "$name")
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
  done
done
