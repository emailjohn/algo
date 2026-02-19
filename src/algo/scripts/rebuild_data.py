from pathlib import Path

from algo.config import settings
from algo.data.prices import update_all_prices, export_canonical_ohlcv


def main() -> None:
    print("Rebuilding data...")

    raw_dir = settings.data_dir / "raw_prices"
    canonical_file = settings.data_dir / "canonical" / "ohlcv.parquet"

    # Remove raw cache if it exists
    if raw_dir.exists():
        print(f"Removing raw cache: {raw_dir}")
        for child in raw_dir.glob("*"):
            if child.is_dir():
                for sub in child.glob("*"):
                    sub.unlink(missing_ok=True)
                child.rmdir()
        raw_dir.rmdir()

    # Remove canonical file if it exists
    if canonical_file.exists():
        print(f"Removing canonical file: {canonical_file}")
        canonical_file.unlink()

    print("Updating all prices...")
    used = update_all_prices()

    print("Providers used:")
    for k, v in used.items():
        print(f"  {k}: {v}")

    print("Exporting canonical OHLCV...")
    path = export_canonical_ohlcv()

    print(f"Done. Canonical written to: {path}")


if __name__ == "__main__":
    main()
