import sys
from pathlib import Path

import pandas as pd

from algo.data.prices import load_canonical_field
from algo.symbols.registry import list_asset_keys_by_kind
from algo.backtest.engine_fast import run_backtest_fast_daily
from algo.backtest.runs import make_run_dir

from algo.strategies.sma_trend import sma_trend_weights_by_day


# ============================
# CONFIG (edit this in PyCharm)
# ============================

STRATEGY = "sma_trend"

START_DATE = "2005-01-01"
WINDOW = 200

KINDS = {"equity", "etf"}
FIELD = "adj_close"

RUN_NAME = "test_run"


# ============================
# Strategy dispatch
# ============================


def build_weights(strategy: str, prices: pd.DataFrame) -> pd.DataFrame:
    """
    Given strategy name + price history,
    return weights_by_day (date × asset).
    """
    if strategy == "sma_trend":
        return sma_trend_weights_by_day(prices, window=WINDOW)

    raise ValueError(f"Unknown strategy: {strategy}")


def main() -> None:
    # universe
    assets = list_asset_keys_by_kind(KINDS)

    # load prices
    px = load_canonical_field(FIELD)[assets].sort_index()

    # build weights
    wmat = build_weights(STRATEGY, px)

    # run backtest
    eq = run_backtest_fast_daily(px, wmat, start_date=START_DATE)

    # save artifacts
    run_dir = make_run_dir(RUN_NAME)
    eq.to_frame().to_parquet(run_dir / "equity.parquet")

    print("strategy:", STRATEGY)
    print("assets:", assets)
    print("start:", eq.index.min())
    print("end:", eq.index.max())
    print("final equity:", float(eq.iloc[-1]))
    print("saved to:", run_dir)


if __name__ == "__main__":
    main()
