import pandas as pd
import numpy as np


def compute_stats(equity_curve: pd.Series, trading_days: int = 252) -> dict[str, float]:
    """
    Beregner de vigtigste nøgletal for en equity curve (der starter på 1.0).
    """
    if equity_curve.empty:
        return {}

    # Daglige afkast
    rets = equity_curve.pct_change().dropna()

    # CAGR (Årligt gennemsnitligt afkast)
    years = len(rets) / trading_days
    cagr = (equity_curve.iloc[-1] ** (1 / years)) - 1 if years > 0 else 0.0

    # Årlig Volatilitet (Risiko)
    vol = rets.std() * np.sqrt(trading_days)

    # Sharpe Ratio (Antager 0% risikofri rente for simpelhedens skyld)
    sharpe = cagr / vol if vol > 0 else 0.0

    # Maximum Drawdown (Hvor stort var det største dyk fra toppen?)
    roll_max = equity_curve.cummax()
    drawdown = (equity_curve - roll_max) / roll_max
    max_dd = drawdown.min()

    return {
        "CAGR": cagr,
        "Vol": vol,
        "Sharpe": sharpe,
        "MaxDD": max_dd
    }