import argparse
from pathlib import Path

from pipeline.core import transform


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Azure batch ETL reference pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="process a CSV batch")
    run_parser.add_argument("--input", type=Path, required=True, help="path to input CSV")
    run_parser.add_argument("--output", type=Path, required=True, help="output root directory")
    args = parser.parse_args()

    manifest = transform(args.input, args.output)
    print(
        f"Batch {manifest['batch_id']}: {manifest['accepted_rows']} accepted, "
        f"{manifest['rejected_rows']} rejected; outputs written to {args.output}"
    )


if __name__ == "__main__":
    main()
