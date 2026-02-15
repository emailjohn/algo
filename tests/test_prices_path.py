from algo.data.prices import raw_cache_path


def test_raw_cache_path():
    path = raw_cache_path("stooq", "spy")
    assert r"algo\data\raw_prices\stooq\spy.parquet" in str(path)
