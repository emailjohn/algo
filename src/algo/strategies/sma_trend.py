import pandas as pd

def sma_trend_weights_by_day(prices: pd.DataFrame, *, window: int = 200) -> pd.DataFrame:
    """
    Compute target weights each day (simple loop; fine for research).
    Returns a DataFrame aligned to prices: date × asset.
    """
    prices = prices.sort_index()

    # 1. Udregn 200 dages snit for HELE dataframen på én gang
    sma = prices.rolling(window=window, min_periods=window).mean()

    # 2. Skab en Sand/Falsk matrix: Hvilke aktier er over deres snit?
    # Bliver til 1.0 (Sand) og 0.0 (Falsk)
    signal_matrix = (prices > sma).astype(float)

    # 3. Fordel vægten ligeligt
    # Hvor mange aktier er "Sand" i dag? (sum langs rækken)
    active_count = signal_matrix.sum(axis=1)

    # Divider 1.0 (Sand) med antallet af aktive aktier for at få %-vægt.
    # Hvis 0 aktier er aktive, erstatter vi division-med-nul (NaN) med 0.
    weights_matrix = signal_matrix.div(active_count, axis=0).fillna(0.0)

    return weights_matrix
