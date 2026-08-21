import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
root = Path(__file__).parent
sys.path.insert(0, str(root))
