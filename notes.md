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

python src\algo\scripts\rebuild_data.py --force