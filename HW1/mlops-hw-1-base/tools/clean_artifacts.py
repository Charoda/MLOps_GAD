import shutil
from pathlib import Path

root = Path(__file__).resolve().parents[1]
shutil.rmtree(root / "include" / "artifacts", ignore_errors=True)
for name in ("__pycache__", ".pytest_cache"):
    for directory in root.rglob(name):
        if directory.is_dir():
            shutil.rmtree(directory)
