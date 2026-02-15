from typing import Literal

# --- Common domain types -----------------------------------------

# Portfolio weights: asset_key -> weight (sum ~ 1.0)
type Weights = dict[str, float]

# Rebalance frequency
Frequency = Literal["D", "W", "M"]
