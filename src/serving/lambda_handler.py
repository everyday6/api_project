"""API Gateway(HTTP API) → Lambda 진입점.

FastAPI 앱(src/serving/nav_api.py)은 그대로 두고 Mangum으로 ASGI 요청을
Lambda 이벤트/응답 포맷으로 변환한다. 라우팅·fallback 로직은 nav_api.py/
nav_lookup.py에 그대로 있다 — 이 파일은 어댑터일 뿐이다.
"""

from __future__ import annotations

from mangum import Mangum

from src.serving.nav_api import app

handler = Mangum(app)
