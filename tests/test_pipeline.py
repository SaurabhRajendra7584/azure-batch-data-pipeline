import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.core import transform

ROOT = Path(__file__).resolve().parents[1]


class PipelineTests(unittest.TestCase):
    def test_sample_batch_accounts_for_every_row_and_builds_gold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            manifest = transform(ROOT / "data" / "sample" / "trips.csv", output)

            self.assertEqual(manifest["input_rows"], 5)
            self.assertEqual(manifest["accepted_rows"], 4)
            self.assertEqual(manifest["rejected_rows"], 1)
            self.assertTrue((output / "bronze" / "trips.csv").is_file())
            with (output / "gold" / "route_service_daily.csv").open(
                newline="", encoding="utf-8"
            ) as source:
                gold = list(csv.DictReader(source))
            self.assertEqual(len(gold), 2)
            self.assertEqual(gold[0]["trip_count"], "3")
            self.assertEqual(gold[0]["on_time_percentage"], "66.67")

    def test_duplicate_trip_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "trips.csv"
            source.write_text(
                (ROOT / "data" / "sample" / "trips.csv").read_text(encoding="utf-8")
                + "T001,2025-01-15,R1,08:00,08:02,08:30,08:36,12\n",
                encoding="utf-8",
            )
            manifest = transform(source, root / "output")

            self.assertEqual(manifest["rejected_rows"], 2)
            with (root / "output" / "silver" / "rejected.csv").open(
                newline="", encoding="utf-8"
            ) as rejected_file:
                reasons = [row["reason"] for row in csv.DictReader(rejected_file)]
            self.assertIn("duplicate trip_id", reasons)

    def test_overnight_arrival_delay_uses_scheduled_arrival(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "trips.csv"
            source.write_text(
                "trip_id,service_date,route_id,scheduled_departure,actual_departure,"
                "scheduled_arrival,actual_arrival,passenger_count\n"
                "N001,2025-01-15,N1,23:50,23:55,00:10,00:25,4\n",
                encoding="utf-8",
            )
            transform(source, root / "output")

            with (root / "output" / "silver" / "trips.csv").open(
                newline="", encoding="utf-8"
            ) as silver_file:
                row = next(csv.DictReader(silver_file))
            self.assertEqual(row["arrival_delay_minutes"], "15")


if __name__ == "__main__":
    unittest.main()
