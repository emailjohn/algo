import pandas as pd

from algo.data.prices import (
    load_canonical_field,
    load_canonical_ohlcv,
    read_cache,
)

# ============================
# CONFIG – edit in PyCharm
# ============================

ASSET = "novo-b-co"

MODE = "canonical"   # "canonical" or "raw"
PROVIDER = "yahoo"   # only used if MODE == "raw"

FIELD = "adj_close"  # used if canonical field mode
SHOW = "ohlcv"       # "field" or "ohlcv"

START = "2010-05-10"
END = None

HEAD = 15
TAIL = None


# ============================


def inspect():
    if MODE == "canonical":
        if SHOW == "field":
            df = load_canonical_field(FIELD)
            if ASSET not in df.columns:
                raise KeyError(f"{ASSET} not found in canonical field '{FIELD}'")
            out = df[ASSET]

        elif SHOW == "ohlcv":
            df = load_canonical_ohlcv()
            if ASSET not in df.columns.get_level_values("asset"):
                raise KeyError(f"{ASSET} not found in canonical OHLCV")
            out = df[ASSET]  # gives DataFrame with ohlcv columns

        else:
            raise ValueError("SHOW must be 'field' or 'ohlcv'")

    elif MODE == "raw":
        df = read_cache(PROVIDER, ASSET)
        if df is None:
            raise ValueError(f"No raw cache for {ASSET} from {PROVIDER}")
        out = df

    else:
        raise ValueError("MODE must be 'canonical' or 'raw'")

    # date slicing
    if START is not None:
        out = out.loc[pd.to_datetime(START):]
    if END is not None:
        out = out.loc[:pd.to_datetime(END)]

    if HEAD:
        print(out.head(HEAD))
    if TAIL:
        print(out.tail(TAIL))

    return out


if __name__ == "__main__":
    inspect()
