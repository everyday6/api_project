"""
서빙 fallback 계층(tier) 요약 로깅.

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
"""

from __future__ import annotations

import logging


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
    known_tiers 순서대로 집계해 한 줄 로깅한다.

    extra는 type1처럼 태그 뒤에 고정 필드(`type=1`)를 하나 더 박아야 하는
    경우에만 쓴다 - 그 외 타입은 태그 자체(`type2_fallback_tier_summary`
    등)에 이미 타입이 들어있어 extra가 필요 없다."""
    counts = {tier: 0 for tier in known_tiers}
    for tier in tiers:
        counts[tier] += 1

    fields = " ".join(f"{tier}={counts[tier]}" for tier in known_tiers)
    prefix = f"[{tag}] {extra}" if extra else f"[{tag}]"
    logger.info(f"{prefix} {fields} total={len(tiers)}")
