"""계산된 Gold 값에 "어떤 공식으로 만들어졌는지"를 새기는 공통 스탬프.

RELIABILITY_PRINCIPLES.md Tier 1 #1 (Lineage/Reproducibility) - 나중에 계산
공식에서 버그가 발견됐을 때, 저장된 값 중 어느 것이 그 공식으로 만들어졌는지
특정할 수 있어야 한다.

버전 문자열은 두 부분이다: `<라벨>+<핵심상수 해시>`
- **라벨**: 사람이 관리하는 변경 이력. 로직 구조(가중치 방식, 특수 케이스
  처리 등)를 바꿀 때 호출부에서 `v1 -> v2`로 올리고, 그 상수 옆 주석에
  무엇이 바뀌었는지 한 줄 남긴다.
- **해시**: 그 공식의 출력을 직접 바꾸는 튜닝값(예: 스무딩 윈도우 크기)들의
  짧은 해시. 그 값을 바꾸면 라벨을 안 올려도 버전이 자동으로 달라져서,
  저장된 값이 코드와 조용히 어긋나는 일을 막는다.

src/common/suspect.py처럼 도메인마다 재구현하지 않게 여기 하나로 둔다 -
nav_time gold2(type1 avg)가 쓰고, tlc gold2(type3 rolling 평균)도 같은
스탬프를 쓸 수 있다.
"""

from __future__ import annotations

import hashlib


def formula_version(label: str, *salient_values: object) -> str:
    """`<label>+<hash>` 형태의 공식 버전 문자열을 만든다.

    salient_values에는 그 공식의 출력을 바꾸는 튜닝값을 넘긴다(예: 스무딩
    윈도우 크기). 순서·값이 바뀌면 해시가 바뀐다. 해시는 6자만 쓴다 -
    저장 컬럼/로그에서 읽기 쉬운 게 중요하고, 실제 대조는 라벨의 변경
    이력으로 하므로 충돌 가능성은 문제되지 않는다.
    """
    digest = hashlib.sha256(
        "|".join(repr(value) for value in salient_values).encode("utf-8")
    ).hexdigest()[:6]
    return f"{label}+{digest}"
