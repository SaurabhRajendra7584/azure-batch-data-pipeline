import csv
import json
import shutil
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

INPUT_COLUMNS = (
    "trip_id",
    "service_date",
    "route_id",
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
    "passenger_count",
)
SILVER_COLUMNS = INPUT_COLUMNS + ("arrival_delay_minutes", "on_time")
GOLD_COLUMNS = (
    "service_date",
    "route_id",
    "trip_count",
    "average_arrival_delay_minutes",
    "on_time_percentage",
)


def _parse_clock(value: str, column: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid {column}: expected HH:MM") from exc


def transform(input_path: Path, output_path: Path) -> dict[str, Any]:
    if not input_path.is_file():
        raise FileNotFoundError(f"input file does not exist: {input_path}")

    output_path.mkdir(parents=True, exist_ok=True)
    bronze_dir = output_path / "bronze"
    silver_dir = output_path / "silver"
    gold_dir = output_path / "gold"
    for directory in (bronze_dir, silver_dir, gold_dir):
        directory.mkdir(parents=True, exist_ok=True)
    shutil.copy2(input_path, bronze_dir / input_path.name)

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    with input_path.open(newline="", encoding="utf-8-sig") as source:
        reader = csv.DictReader(source)
        missing_columns = set(INPUT_COLUMNS) - set(reader.fieldnames or ())
        if missing_columns:
            raise ValueError(f"input is missing required columns: {', '.join(sorted(missing_columns))}")

        for row_number, raw in enumerate(reader, start=2):
            row = {key: (raw.get(key) or "").strip() for key in INPUT_COLUMNS}
            try:
                if any(not row[key] for key in INPUT_COLUMNS):
                    raise ValueError("required value is empty")
                if row["trip_id"] in seen_ids:
                    raise ValueError("duplicate trip_id")
                date.fromisoformat(row["service_date"])
                scheduled_departure = _parse_clock(row["scheduled_departure"], "scheduled_departure")
                actual_departure = _parse_clock(row["actual_departure"], "actual_departure")
                scheduled_arrival = _parse_clock(row["scheduled_arrival"], "scheduled_arrival")
                actual_arrival = _parse_clock(row["actual_arrival"], "actual_arrival")
                passenger_count = int(row["passenger_count"])
                if passenger_count < 0:
                    raise ValueError("passenger_count must be zero or greater")

                scheduled_arrival_offset = scheduled_arrival.hour * 60 + scheduled_arrival.minute
                actual_arrival_offset = actual_arrival.hour * 60 + actual_arrival.minute
                if scheduled_arrival < scheduled_departure:
                    scheduled_arrival_offset += 24 * 60
                if actual_arrival < actual_departure:
                    actual_arrival_offset += 24 * 60
                delay = actual_arrival_offset - scheduled_arrival_offset
                accepted.append(
                    {
                        **row,
                        "passenger_count": passenger_count,
                        "arrival_delay_minutes": delay,
                        "on_time": delay <= 5,
                    }
                )
                seen_ids.add(row["trip_id"])
            except (ValueError, OverflowError) as exc:
                rejected.append({"source_row": str(row_number), "reason": str(exc), **row})

    _write_csv(silver_dir / "trips.csv", SILVER_COLUMNS, accepted)
    _write_csv(silver_dir / "rejected.csv", ("source_row", "reason", *INPUT_COLUMNS), rejected)
    gold = _aggregate(accepted)
    _write_csv(gold_dir / "route_service_daily.csv", GOLD_COLUMNS, gold)

    manifest = {
        "batch_id": uuid4().hex,
        "input_file": input_path.name,
        "input_rows": len(accepted) + len(rejected),
        "accepted_rows": len(accepted),
        "rejected_rows": len(rejected),
        "gold_rows": len(gold),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (output_path / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.DictWriter(destination, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["service_date"], row["route_id"])].append(row)

    result = []
    for (service_date, route_id), trips in sorted(grouped.items()):
        on_time_count = sum(trip["on_time"] for trip in trips)
        result.append(
            {
                "service_date": service_date,
                "route_id": route_id,
                "trip_count": len(trips),
                "average_arrival_delay_minutes": round(
                    sum(trip["arrival_delay_minutes"] for trip in trips) / len(trips), 2
                ),
                "on_time_percentage": round(on_time_count * 100 / len(trips), 2),
            }
        )
    return result
