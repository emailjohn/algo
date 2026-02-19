from datetime import datetime
from pathlib import Path

from algo.config import settings

def make_run_dir(prefix: str) -> Path:
    """
   Create a unique run directory under artifacts/backtests/.
   prefix: e.g. "sma_trend"
   """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir = settings.artifacts_dir / "backtests" / f"{ts}_{prefix}"
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir