# ruff workflow (før commit)

uv run ruff format .
uv run ruff check . --fix
uv run ruff check .

# pyright

uv run pyright

# tests

uv run pytest
