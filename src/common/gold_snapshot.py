"""
Gold 서빙 데이터의 "마지막으로 성공한 값" 스냅샷.

RDS(src/common/rds.py)가 완전히 응답 불가능할 때 쓰는 폴백
(src/serving/nav_lookup.py 참고) - S3는 이미 멀티 AZ로 복제되는 관리형
스토리지라 RDS(Multi-AZ 안 쓰면 단일 인스턴스)보다 죽기 어렵다.

세그먼트당 AVG/SPEC/가장 최근 exact 값만 담는다(하루치 버킷 이력 전부는
안 담음 - 스냅샷을 쓰는 시점엔 오래된 실측값도 어차피 freshness 기준을
넘겨 못 쓰므로 최신 1개면 충분하고, 그만큼 스냅샷 크기가 작아진다).

Gold 파이프라인이 RDS에 쓰기 성공할 때마다 RDS의 현재 상태를 그대로
다시 내보내는 방식이다(부분 병합이 아니라 매번 전체 재수출) - 여러
파이프라인(30분 주기 실시간 버킷, 분기 SPEC)이 같은 스냅샷 파일에 부분
병합을 시도하면 경합으로 값이 유실될 수 있는데, "RDS 상태를 그대로
다시 내보내기"는 그 경합 자체가 없다.
"""

from __future__ import annotations

import json

from src.common.config import GOLD_CACHE_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="gold_snapshot")


def snapshot_path(type_name: str):
    return GOLD_CACHE_DIR / f"{type_name}_snapshot.json"


def write_snapshot(type_name: str, snapshot: dict[str, dict]) -> None:
    """snapshot은 segment_id -> {"avg", "spec", "exact_value", "exact_observed_at"}
    매핑(각 키는 값이 있을 때만 존재)."""
    path = snapshot_path(type_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot))
    logger.info(f"[gold_snapshot] {type_name} 스냅샷 저장 완료: {len(snapshot)}개 세그먼트 -> {path}")


def read_snapshot(type_name: str) -> dict[str, dict]:
    """스냅샷 파일이 없거나 읽기/파싱에 실패하면 빈 dict를 반환한다 -
    호출부(nav_lookup)가 이걸 "폴백도 못 씀"으로 처리해서 하드코딩
    상수로 넘어가게 한다. "무조건 응답" 원칙상 이 최후의 안전망에서
    예외를 던지면 안 된다."""
    path = snapshot_path(type_name)
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text())
    except Exception:
        logger.exception(f"[gold_snapshot] {type_name} 스냅샷 읽기 실패")
        return {}
