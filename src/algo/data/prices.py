import io
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
import yfinance as yf

from algo.config import settings
from algo.symbols.registry import get_identifier, has_identifier, list_asset_keys

Provider = Literal["stooq", "yahoo"]


def _asset_key_to_filename(asset_key: str) -> str:
    """
    Convert an asset key into a safe filename.
    """

    return (
        asset_key.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
        .replace("?", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("|", "_")
    )


def raw_cache_path(provider: Provider, asset_key: str) -> Path:
    base = settings.data_dir / "raw_prices" / provider
    base.mkdir(parents=True, exist_ok=True)
    filename = _asset_key_to_filename(asset_key)
    return base / f"{filename}.parquet"


def fetch_stooq_daily(asset_key: str) -> pd.DataFrame:
    stooq_symbol = get_identifier(asset_key, "stooq")

    url = "https://stooq.com/q/d/l/"
    params = {"s": stooq_symbol, "i": "d"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    df["Date"] = pd.to_datetime(df["Date"], utc=False)
    df = df.set_index("Date").sort_index()
    df.index = pd.to_datetime(df.index).normalize()  # standardizes time to midnight

    out = pd.DataFrame(
        {
            "open": pd.to_numeric(df["Open"], errors="coerce"),
            "high": pd.to_numeric(df["High"], errors="coerce"),
            "low": pd.to_numeric(df["Low"], errors="coerce"),
            "close": pd.to_numeric(df["Close"], errors="coerce"),
            "volume": pd.to_numeric(df["Volume"], errors="coerce"),
        }
    )

    # Keep only rows that have a close; others are not useful
    out = out.dropna(subset=["close"])
    return out


def fetch_yahoo_daily(asset_key: str) -> pd.DataFrame:
    yahoo_symbol = get_identifier(asset_key, "yahoo")

    df = yf.download(
        tickers=yahoo_symbol,
        period="max",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df is None or df.empty:
        raise ValueError(f"No Yahoo data for {asset_key} ({yahoo_symbol})")

    # Handle MultiIndex columns like your EURUSD=X example
    if isinstance(df.columns, pd.MultiIndex):
        # pick the slice for this ticker
        df = df.xs(yahoo_symbol, axis=1, level="Ticker")

    out = pd.DataFrame(
        {
            "open": pd.to_numeric(df["Open"], errors="coerce"),
            "high": pd.to_numeric(df["High"], errors="coerce"),
            "low": pd.to_numeric(df["Low"], errors="coerce"),
            "close": pd.to_numeric(df["Close"], errors="coerce"),
            "volume": pd.to_numeric(df["Volume"], errors="coerce"),
        },
        index=pd.to_datetime(df.index),
    )

    if "Adj Close" in df.columns:
        out["adj_close"] = pd.to_numeric(df["Adj Close"], errors="coerce")

    out = out.sort_index()
    out = out.dropna(subset=["close"])
    return out


def read_cache(provider: Provider, asset_key: str) -> pd.DataFrame | None:
    path = raw_cache_path(provider, asset_key)
    if not path.exists():
        return None  # No raw prices saved

    df = pd.read_parquet(path)

    # ensure DateTimeIndex + sorted + unique
    if "date" in df.columns:
        df = df.set_index("date")

    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    return df


def write_cache(provider: Provider, asset_key: str, df: pd.DataFrame) -> None:
    path = raw_cache_path(provider, asset_key)

    out = df.copy()
    out.index = pd.to_datetime(out.index)
    out.index.name = "date"
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]

    out.to_parquet(path)


def merge_prices(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    new = new.copy()
    new.index = pd.to_datetime(new.index)
    new = new.sort_index()
    new = new[~new.index.duplicated(keep="last")]

    if existing is None:
        return new

    merged = pd.concat([existing, new], axis=0)
    merged = merged.sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged


def update_cache_stooq(asset_key: str) -> pd.DataFrame:
    provider: Provider = "stooq"

    existing = read_cache(provider, asset_key)
    fresh = fetch_stooq_daily(asset_key)

    merged = merge_prices(existing, fresh)
    write_cache(provider, asset_key, merged)

    return merged


def update_cache_yahoo(asset_key: str) -> pd.DataFrame:
    provider: Provider = "yahoo"

    existing = read_cache(provider, asset_key)
    fresh = fetch_yahoo_daily(asset_key)

    merged = merge_prices(existing, fresh)
    write_cache(provider, asset_key, merged)

    return merged


def update_cache(provider: Provider, asset_key: str) -> pd.DataFrame:
    if provider == "stooq":
        return update_cache_stooq(asset_key)
    if provider == "yahoo":
        return update_cache_yahoo(asset_key)
    raise ValueError(f"Unknown provider: {provider}")


def _choose_provider_and_update(
    asset_key: str,
    provider_priority: list[Provider],
) -> tuple[Provider, pd.DataFrame]:
    last_err: Exception | None = None

    for provider in provider_priority:
        if not has_identifier(asset_key, provider):
            continue
        try:
            df = update_cache(provider, asset_key)
            return provider, df
        except Exception as e:
            last_err = e

    raise RuntimeError(f"Failed for '{asset_key}'. Last error: {last_err}")


def _select_price_field(df: pd.DataFrame, field: str) -> pd.Series:
    if field in df.columns:
        return df[field]
    if field == "adj_close" and "close" in df.columns:
        return df["close"]
    raise KeyError(f"Field '{field}' not available. Columns: {list(df.columns)}")


def load_prices(
    asset_keys: list[str],
    *,
    field: str = "adj_close",
    provider_priority: list[Provider] | None = None,
) -> pd.DataFrame:
    provider_priority = provider_priority or ["stooq", "yahoo"]

    series_list: list[pd.Series] = []

    for key in asset_keys:
        _provider, df = _choose_provider_and_update(key, provider_priority)

        s = _select_price_field(df, field).rename(key)
        series_list.append(s)

    return pd.concat(series_list, axis=1).sort_index()


def update_all_prices(
    *,
    provider_priority: list[Provider] | None = None,
) -> dict[str, Provider]:
    """
    Update caches for all assets in the registry.
    Returns a dict: asset_key -> provider used.
    """
    provider_priority = provider_priority or ["stooq", "yahoo"]

    used: dict[str, Provider] = {}

    for key in list_asset_keys():
        provider, _df = _choose_provider_and_update(key, provider_priority)
        used[key] = provider

    return used


def canonical_ohlcv_path() -> Path:
    base = settings.data_dir / "canonical"
    base.mkdir(parents=True, exist_ok=True)
    return base / "ohlcv.parquet"


def _canonicalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["open", "high", "low", "close", "volume"]
    out = df.copy()

    # ensure base cols exist (they should for your fetchers)
    missing = [c for c in cols if c not in out.columns]
    if missing:
        raise KeyError(f"Missing columns in OHLCV data: {missing}. Have: {list(out.columns)}")

    # make adj_close always available in canonical dataset
    if "adj_close" not in out.columns:
        out["adj_close"] = out["close"]

    return out[cols + ["adj_close"]]


def export_canonical_ohlcv(
    asset_keys: list[str] | None = None,
    *,
    path: Path | None = None,
    provider_priority: list[Provider] | None = None,
) -> Path:
    """
    Build a canonical OHLCV+adj_close dataset with MultiIndex columns:
      (asset_key, field)

    Writes a single parquet file and returns its path.
    """
    provider_priority = provider_priority or ["stooq", "yahoo"]
    path = path or canonical_ohlcv_path()

    if asset_keys is None:
        asset_keys = list_asset_keys()

    frames: list[pd.DataFrame] = []

    for key in asset_keys:
        _provider, df = _choose_provider_and_update(key, provider_priority)

        canon = _canonicalize_ohlcv(df)
        canon.columns = pd.MultiIndex.from_product(
            [[key], canon.columns],
            names=["asset", "field"],
        )
        frames.append(canon)

    out = pd.concat(frames, axis=1).sort_index()
    out.to_parquet(path)
    return path


def load_canonical_ohlcv(*, path: Path | None = None) -> pd.DataFrame:
    """
    Load the canonical OHLCV+adj_close dataset (MultiIndex columns: asset, field).
    """
    path = path or canonical_ohlcv_path()
    if not path.exists():
        raise FileNotFoundError(
            f"Canonical OHLCV file not found at {path}. Run export_canonical_ohlcv() first."
        )

    df = pd.read_parquet(path)
    df.index = pd.to_datetime(df.index)
    return df.sort_index()


def load_canonical_field(
    field: str,
    *,
    path: Path | None = None,
) -> pd.DataFrame:
    df = load_canonical_ohlcv(path=path)

    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError("Canonical OHLCV is expected to have MultiIndex columns (asset, field).")

    if field not in df.columns.get_level_values("field"):
        available = sorted(set(df.columns.get_level_values("field")))
        raise KeyError(f"Field '{field}' not found. Available fields: {available}")

    out = df.xs(field, axis=1, level="field")

    # pandas typing: xs can be DataFrame or Series; we expect DataFrame here
    if isinstance(out, pd.Series):
        raise TypeError("Expected DataFrame from xs(), got Series. Column structure may be wrong.")

    return out.sort_index()
