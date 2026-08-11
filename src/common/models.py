"""
TLC 모델 정의
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DownloadFile:
    """
    다운로드 대상 파일 정보
    """

    taxi_type: str
    filename: str
    url: str


@dataclass
class DownloadResult:
    """다운로드 결과"""

    taxi_type: str
    filename: str
    tmp_path: Path

@dataclass
class BronzeResult:
    """Bronze 저장 결과"""

    taxi_type: str
    filename: str
    bronze_path: Path
    is_new: bool


@dataclass
class SilverResult:
    """Silver 저장 결과"""

    filename: str
    silver_path: Path