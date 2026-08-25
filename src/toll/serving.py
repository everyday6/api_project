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

from src.common import db
from src.common.config import SERVING_TABLE_TYPE4


def get_toll_value(segment_id: str) -> float:
    """서빙 조회 함수(단건). 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음). gold.py 등 기존 호출부 하위
    호환용으로 남겨둔다 — 여러 세그먼트를 한 번에 조회할 때는
    get_toll_values()를 쓴다(RDS 호출 횟수를 줄임)."""

    return db.get_value(
        SERVING_TABLE_TYPE4,
        {"segment_id": segment_id},
        "value",
        default=0,
    )


def get_toll_values(segment_ids: list[str]) -> list[float]:
    """서빙 조회 함수(배치). segment_ids 순서/중복을 그대로 유지해서
    반환한다 - 중복 제거 후 한 번에 조회하고 원래 순서로 다시 매핑한다.

    RDS 호출 자체가 실패하면(커넥션/네트워크 등) 전부 0으로 응답한다
    - 무조건 응답 원칙, 통행료 하나 때문에 전체 요청이 죽으면 안 된다."""

    unique_ids = list(dict.fromkeys(segment_ids))
    keys = [{"segment_id": segment_id} for segment_id in unique_ids]

    try:
        found = db.batch_get_items(SERVING_TABLE_TYPE4, keys)
    except Exception:
        return [0.0] * len(segment_ids)

    values = {
        segment_id: float(found[(segment_id,)].get("value", 0))
        for segment_id in unique_ids
        if (segment_id,) in found
    }
    return [values.get(segment_id, 0.0) for segment_id in segment_ids]
