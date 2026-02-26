import argparse

from algo.config import settings
from algo.data.cleaning import build_cleaned_ohlcv
from algo.data.prices import export_canonical_ohlcv, update_all_prices


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild or update data pipeline.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete all data (raw, canonical and cleaned) and get it from scratch",
    )
    args = parser.parse_args()

    raw_dir = settings.data_dir / "raw_prices"
    canonical_file = settings.data_dir / "canonical" / "ohlcv.parquet"
    cleaned_file = settings.data_dir / "cleaned" / "ohlcv.parquet"
    cleaned_eligibility_file = settings.data_dir / "cleaned" / "eligibility.parquet"

    if args.force:
        # Remove raw cache if it exists
        if raw_dir.exists():
            for child in raw_dir.glob("*"):
                if child.is_dir():
                    for sub in child.glob("*"):
                        sub.unlink(missing_ok=True)
                    child.rmdir()
            raw_dir.rmdir()

        if canonical_file.exists():
            print(f"Removing canonical file: {canonical_file}")
            canonical_file.unlink()
        if cleaned_file.exists():
            print(f"Removing cleaned file: {cleaned_file}")
            cleaned_file.unlink()
        if cleaned_eligibility_file.exists():
            print(f"Removing cleaned_eligibility file: {cleaned_eligibility_file}")
            cleaned_eligibility_file.unlink()

    print("Updating all prices...")
    used = update_all_prices()

    print("Providers used:")
    for k, v in used.items():
        print(f"  {k}: {v}")

    print("Exporting canonical OHLCV...")
    path = export_canonical_ohlcv()

    print(f"Done. Canonical written to: {path}")

    print("Building cleaned dataset...")
    build_cleaned_ohlcv()
    print("Cleaning complete.")


if __name__ == "__main__":
    main()
