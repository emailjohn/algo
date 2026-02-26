import duckdb
import pandas as pd

from algo.data.cleaning import eligibility_path
from algo.symbols.registry import get_registry


def get_clean_universe(
    kinds: set[str] | None = None, min_coverage: float = 0.98, max_extreme: int = 0
) -> list[str]:
    """
    Filtrerer universe via DuckDB SQL ved at joine Python Registry med Parquet-data.
    """
    reg = get_registry()
    path = str(eligibility_path())

    # 1. Konverter registry til en Pandas DataFrame i stedet for bare en liste
    meta_list = [{"asset": a.key, "kind": a.kind} for a in reg.assets]
    assets_meta = pd.DataFrame(meta_list)  # noqa: F841

    # 2. Base query der joiner Pandas DataFrame med Parquet-filen på disken
    query = f"""
        SELECT meta.asset
        FROM assets_meta AS meta
        JOIN read_parquet('{path}') AS elig 
          ON meta.asset = elig.asset
        WHERE elig.coverage_ratio >= {min_coverage}
          AND elig.extreme_count <= {max_extreme}
    """

    # 3. Tilføj dynamisk filtrering på 'kinds' hvis angivet
    if kinds:
        kinds_sql = ", ".join(f"'{k}'" for k in kinds)
        query += f" AND meta.kind IN ({kinds_sql})"

    # 4. Kør query og træk resultatet ud som en Python list
    result_df = duckdb.sql(query).df()

    return result_df["asset"].tolist()
