# ruff workflow (før commit)

uv run ruff format .
uv run ruff check . --fix
uv run ruff check .

# pyright

uv run pyright

# tests

uv run pytest

# pre-commit

uv run pre-commit run --all-files

# update prices
Remove-Item -Recurse -Force .\data\raw_prices
Remove-Item -Force .\data\canonical\ohlcv.parquet
uv run python -c "import sys; sys.path.append('src'); from algo.data.prices import update_all_prices, export_canonical_ohlcv; update_all_prices(); export_canonical_ohlcv(); print('done')"
