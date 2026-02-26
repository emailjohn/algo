from pathlib import Path

import pandas as pd

from algo.config import settings
from algo.data.prices import load_canonical_ohlcv

# ==========================
# Default parameters
# ==========================

GLOBAL_FLOOR = pd.Timestamp("2005-01-01")  # global start date for all time series
MINIMUM_DAYS = 1000  # minimum length of a time series for a given asset to be valid
EXTREME_THRESHOLD = 0.60
COVERAGE_LIMIT = 0.98
STABLE_WINDOW = 252
FFILL_LIMIT = 5


# ==========================
# Core logic
# ==========================


def ffill_small_gaps_only(s: pd.Series, *, max_gap: int) -> pd.Series:
    """
    Forward-fill NaN runs only if the *entire* run length <= max_gap.
    If a run is longer, leave the whole run as NaN (no partial fill).
    """
    s = s.copy()
    na = s.isna()
    if not na.any():
        return s

    # group consecutive equal values in na-mask
    grp = (na != na.shift()).cumsum()

    # size of each group
    sizes = grp.groupby(grp).transform("size")

    # fill mask: NaN rows that are in a small NaN-run
    fill_mask = na & (sizes <= max_gap)

    # forward-filled candidate
    filled = s.ffill()

    # only apply fill on small gaps
    s.loc[fill_mask] = filled.loc[fill_mask]
    return s


def _find_auto_start(series: pd.Series) -> pd.Timestamp | None:
    """
    Find first stable start date.
    Stable = next STABLE_WINDOW days:
        - high coverage
        - no extreme returns
    """
    if series.dropna().empty:
        return None

    series_for_rets = ffill_small_gaps_only(series, max_gap=FFILL_LIMIT)
    rets = series_for_rets.pct_change()

    dates = series.index

    for i in range(len(dates) - STABLE_WINDOW):
        window = series.iloc[i : i + STABLE_WINDOW]
        window_rets = rets.iloc[i + 1 : i + STABLE_WINDOW + 1]

        coverage = window.notna().mean()
        extreme = (window_rets.abs() > EXTREME_THRESHOLD).any()

        if coverage > COVERAGE_LIMIT and not extreme:
            return dates[i]

    return None


def _clean_single_asset(df: pd.DataFrame, asset: str):
    """
    Clean OHLCV data for single asset.
    """
    asset_df = df[asset].copy()

    # determine start date based on adj_close
    if "adj_close" in asset_df.columns:
        price_series = asset_df["adj_close"]
    else:
        price_series = asset_df["close"]

    auto_start = _find_auto_start(price_series)

    if auto_start is None:
        return None, None

    start = max(GLOBAL_FLOOR, auto_start)

    cleaned = asset_df.loc[start:].copy()

    if len(cleaned) < MINIMUM_DAYS:
        return None, None

    # gap-aware fill (all-or-nothing per gap)
    for col in ["open", "high", "low", "close", "adj_close"]:
        if col in cleaned.columns:
            cleaned[col] = ffill_small_gaps_only(cleaned[col], max_gap=FFILL_LIMIT)

    price_series_clean = (
        cleaned["adj_close"] if "adj_close" in cleaned.columns else cleaned["close"]
    )

    # compute stats
    rets = price_series_clean.pct_change()
    extreme_mask = rets.abs() > EXTREME_THRESHOLD

    info = {
        "asset": asset,
        "auto_start": auto_start,
        "final_start": start,
        "extreme_count": int(extreme_mask.sum()),
        "last_extreme_date": rets[extreme_mask].index.max() if extreme_mask.any() else None,
        "coverage_ratio": float(price_series.notna().mean()),
    }

    return cleaned, info


# ==========================
# Public API
# ==========================


def cleaned_path() -> Path:
    base = settings.data_dir / "cleaned"
    base.mkdir(parents=True, exist_ok=True)
    return base / "ohlcv.parquet"


def eligibility_path() -> Path:
    base = settings.data_dir / "cleaned"
    base.mkdir(parents=True, exist_ok=True)
    return base / "eligibility.parquet"


def build_cleaned_ohlcv() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build cleaned OHLCV dataset from canonical.
    """
    canonical = load_canonical_ohlcv()

    cleaned_frames = []
    eligibility = []

    for asset in canonical.columns.get_level_values("asset").unique():
        cleaned_asset, info = _clean_single_asset(canonical, asset)

        if cleaned_asset is None:
            continue

        cleaned_asset.columns = pd.MultiIndex.from_product(
            [[asset], cleaned_asset.columns],
            names=["asset", "field"],
        )

        cleaned_frames.append(cleaned_asset)
        eligibility.append(info)

    if not cleaned_frames:
        raise ValueError("No assets survived cleaning.")

    cleaned = pd.concat(cleaned_frames, axis=1).sort_index()
    eligibility_df = pd.DataFrame(eligibility).set_index("asset")

    # ensure datetime columns are proper dtype
    for col in ["auto_start", "final_start", "last_extreme_date"]:
        if col in eligibility_df.columns:
            eligibility_df[col] = pd.to_datetime(eligibility_df[col], errors="coerce").dt.strftime(
                "%Y-%m-%d"
            )

    cleaned.to_parquet(cleaned_path())
    eligibility_df.to_parquet(eligibility_path())

    return cleaned, eligibility_df


def load_cleaned_ohlcv(*, path: Path | None = None) -> pd.DataFrame:
    path = path or cleaned_path()
    df = pd.read_parquet(path)
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def load_cleaned_field(field: str, *, path: Path | None = None) -> pd.DataFrame:
    df = load_cleaned_ohlcv(path=path)
    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError("Expected MultiIndex columns (asset, field)")
    out = df.xs(field, axis=1, level="field")
    if isinstance(out, pd.Series):
        raise TypeError("Expected DataFrame, got Series")
    return out.sort_index()
