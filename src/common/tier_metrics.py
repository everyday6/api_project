"""
서빙 fallback 계층(tier) 요약 로깅 + 급증 알림.

nav_lookup.py(type1/2)/api.py(type3)/toll/serving.py(type4)가 각자 요청당
"어느 계층(rds/global/snapshot/hardcoded 등)에서 값을 뽑았는지" 개수를
집계해 한 줄로 로깅하던 동일한 형태의 코드를 공용화한 것이다. 세그먼트마다
로그를 남기면 요청 하나(최대 1,000개 segment)에 수백 줄까지 늘어날 수
있어서, 계층별 개수만 집계해 요청당 한 줄만 남긴다.

로그 문자열 형식(`[tag] [extra] tier1=n1 tier2=n2 ... total=n`)은
CloudWatch Logs Insights가 `parse @message "rds=* snapshot=* ..."`로 그대로
파싱해 Grafana 패널을 그린다(grafana/provisioning/dashboards/
nav-gold-overview.json 참고) - 타입별로 필드 이름/순서가 이미 대시보드에
박제돼 있으므로, 어떤 타입을 통합하더라도 그 타입의 기존 필드 순서를
그대로 known_tiers에 넘겨야 한다. fallback 판단 로직 자체(어떤 순서로 어떤
저장소를 시도할지)는 이 모듈이 관여하지 않는다 - 그건 타입마다 달라서
호출부에 그대로 남아있다.

RELIABILITY_PRINCIPLES.md Tier 3 #10: 교과서적 circuit breaker(실패하는
의존성 호출 자체를 차단)는 이 API엔 안 맞는다 - 폴백 체인이 절대 예외를
던지지 않아서, 호출을 차단해봤자 나쁜 응답을 막는 게 아니라 빠른 타임아웃
한 번을 아낄 뿐이다. 폴백 체인이 이미 containment다. 진짜 갭은
`05-rds-fallback`의 93.58% fallback 사태처럼 **fallback이 뭉텅이로 터졌는데
아무도 페이지를 못 받는 것**이라, 여기서는 "차단"이 아니라 "급증 알림"만
구현한다.
"""

from __future__ import annotations

import logging
import time

from src.common.alerts import notify_slack_message

# 요청의 마지막 계층(known_tiers[-1] = 항상 `hardcoded`, 코드 상수)이 차지하는
# 비율이 이 값을 넘으면 Slack 알림을 보낸다. `hardcoded`는 RDS도 스냅샷도
# 실패했다는 뜻이라 어느 타입에서든 명백한 장애 신호다(`avg`/`snapshot`/
# `global`은 설계된 중간 폴백이라 정상적으로도 높을 수 있어 알림 기준이 아니다).
#
# NOTE: placeholder. 실제 트래픽의 baseline hardcoded 비율을 측정한 뒤
# 조정해야 한다 - 아직 실측 근거가 없다.
FALLBACK_ALERT_RATIO = 0.10

# segment 몇 개짜리 요청(전부 hardcoded여도 신호가 아님)에는 알림을 안 낸다.
_MIN_TOTAL_FOR_ALERT = 20

# 같은 tag로 이 시간(초) 안에는 알림을 한 번만 보낸다 - 장애 중 매 요청마다
# 알림이 폭주하는 걸 막는다. 상태는 프로세스 로컬이라(Lambda 웜 인스턴스)
# 광범위 장애 시 인스턴스당·창당 최대 1건씩 온다 - 침묵보다 낫고 폭주도 아니다.
_ALERT_COOLDOWN_SECONDS = 600

_last_alert_at: dict[str, float] = {}


def _maybe_alert_fallback_spike(
    logger: logging.Logger,
    tag: str,
    counts: dict[str, int],
    known_tiers: list[str],
    total: int,
    fields: str,
    extra: str,
) -> None:
    if total < _MIN_TOTAL_FOR_ALERT:
        return

    terminal_tier = known_tiers[-1]
    ratio = counts[terminal_tier] / total
    if ratio <= FALLBACK_ALERT_RATIO:
        return

    now = time.monotonic()
    if now - _last_alert_at.get(tag, 0.0) < _ALERT_COOLDOWN_SECONDS:
        return
    _last_alert_at[tag] = now

    label = f"{tag} {extra}".strip()
    try:
        notify_slack_message(
            f":warning: 서빙 fallback 급증 - `{label}`: "
            f"{terminal_tier}={ratio:.0%} ({counts[terminal_tier]}/{total})\n"
            f"코드 상수({terminal_tier})로 응답하는 비율이 임계치"
            f"({FALLBACK_ALERT_RATIO:.0%})를 넘었습니다 - RDS/스냅샷 상태 확인 필요\n"
            f"*계층 분포*: {fields}"
        )
    except Exception:
        # 알림 실패가 서빙 응답을 깨면 안 된다(hot path).
        logger.exception("fallback 급증 Slack 알림 전송 실패: %s", label)


def log_tier_summary(
    logger: logging.Logger,
    tag: str,
    tiers: list[str],
    known_tiers: list[str],
    *,
    extra: str = "",
) -> None:
    """`tiers`(id 하나당 계층 문자열 하나, 순서/개수는 원래 요청의 id
    목록과 동일 - 같은 id가 반복돼도 발생 횟수만큼 그대로 넘긴다)를
    known_tiers 순서대로 집계해 한 줄 로깅하고, 마지막 계층(코드 상수)
    비율이 임계치를 넘으면 Slack 알림을 보낸다(rate-limited).

    extra는 type1처럼 태그 뒤에 고정 필드(`type=1`)를 하나 더 박아야 하는
    경우에만 쓴다 - 그 외 타입은 태그 자체(`type2_fallback_tier_summary`
    등)에 이미 타입이 들어있어 extra가 필요 없다."""
    counts = {tier: 0 for tier in known_tiers}
    for tier in tiers:
        counts[tier] += 1

    fields = " ".join(f"{tier}={counts[tier]}" for tier in known_tiers)
    prefix = f"[{tag}] {extra}" if extra else f"[{tag}]"
    logger.info(f"{prefix} {fields} total={len(tiers)}")

    _maybe_alert_fallback_spike(
        logger, tag, counts, known_tiers, len(tiers), fields, extra
    )
