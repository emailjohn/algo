# scripts/extreme_returns_summary.py
#
# Compact per-asset overview of extreme daily returns in canonical prices.
# Prints a small summary table + optional top-N worst/best events per asset.
#
# Run:
#   uv run python scripts/extreme_returns_summary.py

import pandas as pd

from algo.data.prices import load_canonical_field
from algo.symbols.registry import list_asset_keys


# ============================
# CONFIG – edit in PyCharm
# ============================

ASSETS = None  # None = all assets, or ["spy", "novo-b-co"]
FIELD = "adj_close"

START = "2000-01-01"
END = None

LOWER = -0.5
UPPER = 0.5

# Print per-asset event lists?
SHOW_TOP_EVENTS = True
TOP_N_WORST = 5
TOP_N_BEST = 5

# Also suggest a "clean start" date (first date after which no extremes occur)?
SUGGEST_CLEAN_START = True

# If true, ignore events before this when suggesting clean start
CLEAN_START_SEARCH_FROM = "1990-01-01"

# ============================


def find_extreme_returns(prices: pd.Series, *, lower: float, upper: float) -> pd.Series:
    r = prices.pct_change()
    return r[(r < lower) | (r > upper)].dropna()


def suggest_clean_start(extreme_dates: pd.DatetimeIndex, *, search_from: str) -> pd.Timestamp | None:
    """
    Suggest the first date after which there are no extreme dates.
    If there are no extremes, returns None.
    """
    if len(extreme_dates) == 0:
        return None

    cutoff = pd.to_datetime(search_from)
    extreme_dates = extreme_dates[extreme_dates >= cutoff]
    if len(extreme_dates) == 0:
        return None

    # If you start after the last extreme, you're "clean"
    return extreme_dates.max() + pd.Timedelta(days=1)


def main() -> None:
    px = load_canonical_field(FIELD).sort_index()

    if START is not None:
        px = px.loc[pd.to_datetime(START):]
    if END is not None:
        px = px.loc[:pd.to_datetime(END)]

    assets = ASSETS if ASSETS is not None else list_asset_keys()

    summary_rows: list[dict] = []

    per_asset_events: dict[str, pd.Series] = {}

    for asset in assets:
        if asset not in px.columns:
            continue

        s = px[asset].dropna().sort_index()
        if s.empty:
            continue

        ext = find_extreme_returns(s, lower=LOWER, upper=UPPER)
        per_asset_events[asset] = ext

        if ext.empty:
            summary_rows.append(
                {
                    "asset": asset,
                    "extreme_days": 0,
                    "worst_ret": None,
                    "worst_date": None,
                    "best_ret": None,
                    "best_date": None,
                    "first_extreme": None,
                    "last_extreme": None,
                    "suggest_clean_start": None,
                }
            )
            continue

        worst = ext.min()
        worst_date = ext.idxmin()
        best = ext.max()
        best_date = ext.idxmax()

        clean_start = None
        if SUGGEST_CLEAN_START:
            clean_start = suggest_clean_start(ext.index, search_from=CLEAN_START_SEARCH_FROM)

        summary_rows.append(
            {
                "asset": asset,
                "extreme_days": int(ext.shape[0]),
                "worst_ret": float(worst),
                "worst_date": worst_date.date().isoformat(),
                "best_ret": float(best),
                "best_date": best_date.date().isoformat(),
                "first_extreme": ext.index.min().date().isoformat(),
                "last_extreme": ext.index.max().date().isoformat(),
                "suggest_clean_start": clean_start.date().isoformat() if clean_start is not None else None,
            }
        )

    if not summary_rows:
        print("No assets found / no data.")
        return

    summary = pd.DataFrame(summary_rows)

    # Sort: most extreme days first, then worst return
    summary = summary.sort_values(
        by=["extreme_days", "worst_ret"],
        ascending=[False, True],
        na_position="last",
    )

    print("\n=== EXTREME RETURNS SUMMARY ===")
    print(f"field={FIELD}  window={START}..{END or 'end'}  thresholds=({LOWER}, {UPPER})")
    print(summary.to_string(index=False))

    if SHOW_TOP_EVENTS:
        for asset, ext in per_asset_events.items():
            if ext.empty:
                continue

            print(f"\n=== {asset}: worst {TOP_N_WORST} ===")
            print(ext.sort_values().head(TOP_N_WORST).to_string())

            print(f"\n=== {asset}: best {TOP_N_BEST} ===")
            print(ext.sort_values(ascending=False).head(TOP_N_BEST).to_string())


if __name__ == "__main__":
    main()
