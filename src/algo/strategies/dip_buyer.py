import pandas as pd

def dip_buyer_weights_by_day(
    prices: pd.DataFrame,
    drop_pct: float = 0.20,
    window: int = 100,           # 63 handelsdage er ca. 3 måneder
    take_profit: float = 1.00,  # Sælg ved +15%
    stop_loss: float = 0.15     # Sælg ved -10%
) -> pd.DataFrame:
    """
    Køber assets der er faldet X% over Y dage.
    Holder indtil Take Profit eller Stop Loss rammes.
    """
    if prices.empty:
        raise ValueError("prices is empty")

    prices = prices.sort_index()
    weights = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)

    # 1. Udregn afkast over de seneste 63 dage (Vektoriseret for hastighed)
    # Giver f.eks. -0.21 hvis aktien er faldet 21% de sidste 3 mdr.
    rolling_drop = prices.pct_change(periods=window)

    # 2. Vores "Hukommelse" (State tracking)
    in_position = {asset: False for asset in prices.columns}
    entry_prices = {asset: 0.0 for asset in prices.columns}

    # 3. Gennemgå historien dag for dag
    for i in range(len(prices)):
        dt = prices.index[i]
        current_prices = prices.iloc[i]
        current_drops = rolling_drop.iloc[i]

        daily_active = []

        for asset in prices.columns:
            p = current_prices[asset]
            if pd.isna(p):
                continue

            # A: HAR VI ALLEREDE AKTIEN?
            if in_position[asset]:
                # Udregn afkast siden vi købte
                ret_since_entry = (p - entry_prices[asset]) / entry_prices[asset]

                # Tjek om vi rammer Take Profit eller Stop Loss
                if ret_since_entry >= take_profit or ret_since_entry <= -stop_loss:
                    in_position[asset] = False
                    entry_prices[asset] = 0.0
                else:
                    daily_active.append(asset) # Behold den!

            # B: HAR VI IKKE AKTIEN? (Tjek om vi skal købe)
            else:
                drop = current_drops[asset]
                # Hvis drop er f.eks. -0.22, og grænsen er -0.20 -> KØB
                if not pd.isna(drop) and drop <= -drop_pct:
                    in_position[asset] = True
                    entry_prices[asset] = p
                    daily_active.append(asset)

        # 4. Fordel vægten ligeligt mellem de aktier vi holder i dag
        if daily_active:
            w = 1.0 / len(daily_active)
            for asset in daily_active:
                weights.loc[dt, asset] = w

    return weights