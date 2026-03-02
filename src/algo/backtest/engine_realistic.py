import pandas as pd


def run_backtest_realistic(
        prices: pd.DataFrame,
        weights_by_day: pd.DataFrame,
        initial_capital: float = 100_000.0,
        commission_pct: float = 0.0015,
        allow_fractional: float = False,  # NY: Hele aktier
        drift_tolerance: float = 0.05  # NY: Tillad 5% afvigelse før vi handler
) -> pd.DataFrame:
    prices = prices.sort_index()
    w = weights_by_day.reindex(index=prices.index, columns=prices.columns).fillna(0.0)
    w = w.shift(1).fillna(0.0)

    cash = initial_capital
    holdings = {asset: 0.0 for asset in prices.columns}
    history = []

    for dt in prices.index:
        current_prices = prices.loc[dt]

        # 1. Beregn porteføljens samlede værdi
        port_value = cash
        for asset in prices.columns:
            p = current_prices[asset]
            if not pd.isna(p):
                port_value += holdings[asset] * p

        history.append({
            "date": dt,
            "total_value": port_value,
            "cash": cash
        })

        # 2. Rebalancering
        target_weights = w.loc[dt]

        for asset in prices.columns:
            p = current_prices[asset]
            if pd.isna(p) or p == 0:
                continue

            # Find nuværende %-vægt af denne aktie i porteføljen
            current_weight = (holdings[asset] * p) / port_value if port_value > 0 else 0.0
            target_weight = target_weights[asset]

            # TJEK DRIFT: Er forskellen mellem nuværende vægt og målet større end tolerancen?
            # Hvis vi skal have 0 (sælge alt), ignorerer vi tolerance og sælger.
            if target_weight != 0 and abs(current_weight - target_weight) < drift_tolerance:
                continue  # Hop over handel, vi er tæt nok på målet!

            target_value = port_value * target_weight
            target_shares = target_value / p

            # TJEK HELE AKTIER
            if not allow_fractional:
                target_shares = int(target_shares)  # Runder ned til nærmeste hele aktie

            delta_shares = target_shares - holdings[asset]

            if abs(delta_shares) > 0:
                trade_value = delta_shares * p
                fee = abs(trade_value) * commission_pct

                cash -= (trade_value + fee)
                holdings[asset] += delta_shares

    result = pd.DataFrame(history).set_index("date")
    result["equity"] = result["total_value"] / initial_capital
    return result