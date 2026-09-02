"""통행료(type4) 서빙 조회 — 배치 파이프라인(gold.py)과 분리된 경량 모듈.

gold.py는 pandas/yaml/geopandas 같은 무거운 배치 처리 의존성을 쓰는데,
Lambda 서빙 이미지(requirements-lambda.txt)엔 그게 없다. get_toll_value()는
RDS 조회 하나뿐이라 그 의존성이 전혀 필요 없는데, nav_api.py가 gold.py에서
그대로 import하면 모듈 전체가 로드되면서 무거운 import까지 실행돼 Lambda
콜드 스타트 자체가 ModuleNotFoundError로 죽는다(실제로 배포 후
/api/navigation/values 전체가 500으로 죽는 것으로 확인됨). 그래서 서빙에
필요한 최소 코드만 여기로 분리한다. gold.py는 이 모듈에서 다시 import해서
기존 호출부(테스트 포함) 하위 호환을 유지한다.
"""

from __future__ import annotations

from src.common import db, gold_snapshot
from src.common.config import SERVING_TABLE_TYPE4
from src.common.logger import get_logger
from src.common.tier_metrics import log_tier_summary
from src.common.utils import unique_in_order
from src.serving import provenance as prov

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_serving")

# RDS 자체가 응답 불가능할 때 대신 쓰는 S3 스냅샷(gold.py의 write_gold_items가
# RDS 쓰기 성공 시마다 갱신). Lambda 웜 인스턴스에서 최초 1회만 로드해서
# 재사용한다 - 통행료 대상 segment만이라 전체를 통째로 담아도 작다.
_snapshot = gold_snapshot.LazySnapshot("type4")


def get_toll_value(segment_id: str) -> float:
    """서빙 조회 함수(단건). 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음). gold.py 등 기존 호출부 하위
    호환용으로 남겨둔다 — 여러 세그먼트를 한 번에 조회할 때는
    get_toll_values()를 쓴다(RDS 호출 횟수를 줄임)."""

    return get_toll_values([segment_id])[0]


def get_toll_values(segment_ids: list[str]) -> list[float]:
    """서빙 조회 함수(배치). 값만 반환하는 얇은 래퍼다 — 값의 출처
    (provenance)까지 필요하면 get_toll_values_with_tiers를 쓴다.
    기존 호출부(gold.py, 테스트 등) 하위 호환용으로 남겨둔다."""

    values, _provenance = get_toll_values_with_tiers(segment_ids)
    return values


def get_toll_values_with_tiers(segment_ids: list[str]) -> tuple[list[float], list[dict]]:
    """서빙 조회 함수(배치). segment_ids 순서/중복을 그대로 유지해서 값과
    각 값의 출처를 구조화된 provenance(`{storage_source, value_basis}`)로
    함께 반환한다(src/serving/provenance.py) - 중복 제거 후 한 번에 조회하고
    원래 순서로 다시 매핑한다. API 응답에 신뢰도를 노출할 때 쓴다
    (RELIABILITY_PRINCIPLES.md 원칙 0-1).

    RDS 호출 자체가 실패하면(커넥션/네트워크 등) S3 스냅샷으로 넘어간다.
    RDS가 정상 응답했는데 특정 segment가 없는 건 "진짜로 통행료 대상이
    아닌 도로"로 간주해서 스냅샷을 거치지 않고 바로 0으로 응답한다 -
    RDS가 멀쩡한데 이미 없다고 확인된 값에 예전 스냅샷을 섞으면 두 실패
    모드(값이 없음 vs RDS가 죽음)가 헷갈린다."""

    unique_ids = unique_in_order(segment_ids)
    keys = [{"segment_id": segment_id} for segment_id in unique_ids]
    token: dict[str, str] = {}

    try:
        found = db.batch_get_items(SERVING_TABLE_TYPE4, keys)
        values = {
            segment_id: float(found[(segment_id,)].get("value", 0))
            for segment_id in unique_ids
            if (segment_id,) in found
        }
        # RDS 호출 자체는 성공했으므로 전부 storage=rds. 다만 "행을 읽어
        # 값을 얻음"(segment_value)과 "행이 없어 0으로 추론함"(implicit_zero,
        # 통행료 대상 아님)은 provenance에서 구분한다 - 예전엔 둘 다 rds로
        # 뭉개졌다.
        for segment_id in unique_ids:
            basis = prov.BASIS_SEGMENT_VALUE if segment_id in values else prov.BASIS_IMPLICIT_ZERO
            token[segment_id] = prov.token(prov.STORAGE_RDS, basis)
    except Exception:
        logger.exception("[toll_serving] RDS 조회 실패 - S3 스냅샷으로 폴백합니다")
        snapshot = _snapshot.get()
        values = {
            segment_id: snapshot[segment_id]
            for segment_id in unique_ids
            if segment_id in snapshot
        }
        for segment_id in unique_ids:
            token[segment_id] = (
                prov.token(prov.STORAGE_S3_SNAPSHOT, prov.BASIS_SEGMENT_VALUE)
                if segment_id in snapshot
                else prov.token(prov.STORAGE_CODE, prov.BASIS_IMPLICIT_ZERO)
            )

    provenance = prov.to_provenance([token[segment_id] for segment_id in segment_ids])
    # 요청당 한 번만 남긴다 - Grafana의 "Type4 fallback 계층 비율" 패널이
    # 이 로그를 예전 평면 어휘(rds/snapshot/hardcoded)로 집계한다.
    log_tier_summary(
        logger,
        "type4_fallback_tier_summary",
        prov.legacy_sources(4, provenance),
        ["rds", "snapshot", "hardcoded"],
        provenance=provenance,
    )

    return [values.get(segment_id, 0.0) for segment_id in segment_ids], provenance
