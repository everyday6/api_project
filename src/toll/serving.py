"""통행료(type4) 서빙 조회 — 배치 파이프라인(gold.py)과 분리된 경량 모듈.

gold.py는 pandas/yaml/geopandas 같은 무거운 배치 처리 의존성을 쓰는데,
Lambda 서빙 이미지(requirements-lambda.txt)엔 그게 없다. get_toll_value()는
DynamoDB 조회 하나뿐이라 그 의존성이 전혀 필요 없는데, nav_api.py가
gold.py에서 그대로 import하면 모듈 전체가 로드되면서 무거운 import까지
실행돼 Lambda 콜드 스타트 자체가 ModuleNotFoundError로 죽는다(실제로
배포 후 /api/navigation/values 전체가 500으로 죽는 것으로 확인됨). 그래서
서빙에 필요한 최소 코드만 여기로 분리한다. gold.py는 이 모듈에서 다시
import해서 기존 호출부(테스트 포함) 하위 호환을 유지한다.
"""

from __future__ import annotations

from src.common import dynamodb
from src.common.config import NAV_GOLD_TABLE

TYPE_TOLL = 4


def get_toll_value(segment_id: str) -> float:
    """서빙 조회 함수. 시설/zone에 해당 안 하는 segment는 0을 반환한다
    (무결점 응답 원칙 — null/에러 없음)."""

    return dynamodb.get_value(NAV_GOLD_TABLE, segment_id, f"TYPE#{TYPE_TOLL}", default=0)
