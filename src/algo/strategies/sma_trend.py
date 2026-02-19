import pandas as pd

from algo.core.types import Weights


def sma_trend_weights(prices: pd.DataFrame, *, window: int = 200) -> Weights:
    """
    For the latest date in `prices`, include asset if price > SMA(window).
    Equal-weight included assets. If none included, returns {}.
    """
    if prices.empty:
        raise ValueError("prices is empty")

    sma = prices.rolling(window=window, min_periods=window).mean()

    latest_price = prices.iloc[-1]
    latest_sma = sma.iloc[-1]

    selected: list[str] = []

    for asset in prices.columns:
        p = latest_price[asset]
        m = latest_sma[asset]

        if pd.isna(p) or pd.isna(m):
            continue

        if p > m:
            selected.append(asset)

    if not selected:
        return {}

    w = 1.0 / len(selected)
    return {asset: w for asset in selected}

def sma_trend_weights_by_day(prices: pd.DataFrame, *, window: int = 200) -> pd.DataFrame:
    """
    Compute target weights each day (simple loop; fine for research).
    Returns a DataFrame aligned to prices: date × asset.
    """
    prices = prices.sort_index()
    out = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)

    for i in range(len(prices.index)):
        hist = prices.iloc[: i + 1]
        w = sma_trend_weights(hist, window=window)
        dt = prices.index[i]
        for k, v in w.items():
            out.loc[dt, k] = v

    return out