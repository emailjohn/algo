import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from algo.data.universe import get_clean_universe
from algo.data.cleaning import load_cleaned_field
from algo.backtest.engine_fast import run_backtest_fast_daily
from algo.backtest.engine_realistic import run_backtest_realistic
from algo.backtest.runs import make_run_dir
from algo.backtest.stats import compute_stats

# ============================
# CONFIG
# ============================
STRATEGY = "dip_buyer"  # Prøv at skifte mellem "sma_trend" og "dip_buyer"
START_DATE = "2013-01-01"
END_DATE = "2026-01-01"
BENCHMARK_ASSET = "spy"

KINDS = {"equity", "etf"}
FIELD = "adj_close"
RUN_NAME = f"{STRATEGY}_test"


def build_weights(strategy_name: str, px: pd.DataFrame) -> pd.DataFrame:
    """
    Dispatcher: Sender dataen til den rigtige strategi-funktion.
    Dette forhindrer NameErrors, fordi vi importerer den rigtige strategi direkte her!
    """
    if strategy_name == "sma_trend":
        from algo.strategies.sma_trend import sma_trend_weights_by_day
        return sma_trend_weights_by_day(px, window=200)

    elif strategy_name == "dip_buyer":
        from algo.strategies.dip_buyer import dip_buyer_weights_by_day
        return dip_buyer_weights_by_day(
            px
        )

    else:
        raise ValueError(f"Ukendt strategi: '{strategy_name}'")


def print_stats(name: str, stats: dict, final_eq: float):
    """Lille hjælpefunktion til at printe stats pænt"""
    print(
        f"{name:15} | Afkast: {final_eq:.2f}x | CAGR: {stats.get('CAGR', 0) * 100:5.1f}% | MaxDD: {stats.get('MaxDD', 0) * 100:6.1f}% | Sharpe: {stats.get('Sharpe', 0):.2f}"
    )


def main() -> None:
    print("1. Henter universe...")
    assets = get_clean_universe(kinds=KINDS, min_coverage=0.5, max_extreme=10)

    if BENCHMARK_ASSET not in assets:
        assets.append(BENCHMARK_ASSET)

    print("2. Loader og slicer priser...")
    px = load_cleaned_field(FIELD)[assets].sort_index()
    px = px.loc[START_DATE:END_DATE]

    print(f"3. Udregner Target Weights for strategi: {STRATEGY}...")
    # HER BRUGER VI DISPATCHEREN I STEDET FOR DET DIREKTE FUNKTIONSKALD:
    wmat = build_weights(STRATEGY, px)

    print("4. Kører Fast Engine...")
    fast_eq = run_backtest_fast_daily(px, wmat)

    print("5. Kører Realistic Engine (Hele aktier + 5% Drift Tolerance)...")
    real_results = run_backtest_realistic(
        px,
        wmat,
        initial_capital=100_000.0,
        allow_fractional=False,
        drift_tolerance=0.05
    )
    real_eq = real_results["equity"]

    print("6. Udregner Benchmark (Buy & Hold)...")
    bench_px = px[BENCHMARK_ASSET].dropna()
    bench_eq = (1.0 + bench_px.pct_change().dropna()).cumprod()

    # Beregn stats
    fast_stats = compute_stats(fast_eq)
    real_stats = compute_stats(real_eq)
    bench_stats = compute_stats(bench_eq)

    print("\n" + "=" * 65)
    print("📈 BACKTEST RESULTATER")
    print("=" * 65)
    print(f"Periode: {px.index.min().strftime('%Y-%m-%d')} til {px.index.max().strftime('%Y-%m-%d')}\n")

    print_stats("Fast Engine", fast_stats, float(fast_eq.iloc[-1]))
    print_stats("Real Engine", real_stats, float(real_eq.iloc[-1]))
    print("-" * 65)
    print_stats(f"Benchmark ({BENCHMARK_ASSET.upper()})", bench_stats, float(bench_eq.iloc[-1]))
    print("=" * 65)

    print("\n7. Gemmer data og genererer graf...")
    run_dir = make_run_dir(RUN_NAME)
    fast_eq.to_frame().to_parquet(run_dir / "fast_equity.parquet")
    real_eq.to_frame().to_parquet(run_dir / "real_equity.parquet")

    plt.style.use('dark_background')
    plt.figure(figsize=(12, 6))

    plt.plot(fast_eq, label=f"Fast Engine ({fast_stats.get('CAGR', 0) * 100:.1f}%)", alpha=0.5, linestyle="--")
    plt.plot(real_eq, label=f"Real Engine ({real_stats.get('CAGR', 0) * 100:.1f}%)", color="cyan", linewidth=2)
    plt.plot(bench_eq, label=f"SPY Benchmark ({bench_stats.get('CAGR', 0) * 100:.1f}%)", color="gray", alpha=0.7)

    plt.title(f"{STRATEGY.replace('_', ' ').title()} vs Benchmark", fontsize=14)
    plt.ylabel("Equity Multiple (Log Scale)")
    plt.yscale("log")
    plt.grid(True, alpha=0.2)
    plt.legend()

    plot_path = run_dir / "equity_curve.png"
    plt.savefig(plot_path)
    print(f"Graf gemt som: {plot_path}")
    plt.show()


if __name__ == "__main__":
    main()