import pandas as pd

def run_backtest_fast_daily(
    prices: pd.DataFrame,
    weights_by_day: pd.DataFrame,
    *,
    start_date: str | None = None,
) -> pd.Series:
    """
    Research backtest with time-varying target weights.

    - prices: wide DataFrame (date × asset)
    - weights_by_day: wide DataFrame (date × asset). Rows may sum <= 1.0.

    Convention:
    weights at date t are applied to returns from t -> t+1 (close-to-close).
    """
    if prices.empty:
        raise ValueError("prices is empty")

    prices = prices.sort_index()

    # optional: clip history
    if start_date is not None:
        prices = prices.loc[pd.to_datetime(start_date):]

    rets = prices.pct_change()

    # align weights to prices
    w = weights_by_day.reindex(index=prices.index).fillna(0.0)
    w = w.reindex(columns=prices.columns).fillna(0.0)

    # avoid lookahead: use weights decided on day t for return t->t+1
    w = w.shift(1).fillna(0.0)

    port_rets = (rets * w).sum(axis=1)
    port_rets = port_rets.dropna()

    equity = (1.0 + port_rets).cumprod()
    equity.name = "equity"
    return equity