import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import replot_recent60_sba as chart


SVG = "{http://www.w3.org/2000/svg}"


class DewPointSpreadTests(unittest.TestCase):
    def setUp(self):
        self.stations = {}
        for index, station_id in enumerate(chart.STATIONS):
            row = chart.blank_station_row(station_id)
            row.update(temp_c=20 + index, dew_c=12, elev_m=100 + index * 150,
                       temp_ob_time="2026-09-23T19:56:00Z", wind_ob_time="2026-09-23T19:56:00Z",
                       wind_dir=220, wind_spd_mps=2, wind_gust_mps=3)
            self.stations[station_id] = row
        self.snapshot = {
            "run_at": "2026-09-23T20:00:00Z", "stations": self.stations,
            "charts": {"metric_svg": "snapshots/20260923T2000Z_metric.svg",
                       "imperial_svg": "snapshots/20260923T2000Z_imperial.svg"},
            "rass": {"points_100m_c": [[200, 23], [400, 22], [600, 21]],
                     "ob_time_utc": "2026-09-23T19:55:00Z", "source": "live"},
        }

    def test_spread_examples_on_graph_and_table_in_both_units(self):
        cases = [
            ("imperial", (63.5 - 32) * 5 / 9, (62.2 - 32) * 5 / 9, "63.5F/+1.3F"),
            ("imperial", (63.5 - 32) * 5 / 9, (63.0 - 32) * 5 / 9, "63.5F/+0.5F"),
            ("imperial", 17.5, 17.5, "63.5F/saturated"),
            ("metric", -5, -5.5, "-5.0C/+0.5C"),
            ("metric", 0, 0, "0.0C/saturated"),
            ("metric", 10, 9.96, "10.0C/saturated"),
            ("imperial", 10, 9.96, "50.0F/+0.1F"),
            ("imperial", 17.5, None, "63.5F/—"),
        ]
        for unit, temperature, dew, expected in cases:
            with self.subTest(unit=unit, temperature=temperature, dew=dew):
                self.stations["SE234"].update(temp_c=temperature, dew_c=dew)
                original = copy.deepcopy(self.snapshot)
                svg = chart.build_snapshot_svgs(self.snapshot)[unit == "imperial"]
                root = ET.fromstring(svg)
                labels = {node[0].text: node[1].text.strip() for node in root.findall(SVG + "text")
                          if node.get("class") == "station-label"}
                self.assertEqual(labels["AntFarm"], expected)
                for node in root.findall(SVG + "text"):
                    if node.get("class") == "legend-row":
                        row = "".join(node.itertext())
                        self.assertIn(labels[row.split(" (")[0]], row)
                self.assertEqual(self.snapshot, original)

    def test_invalid_dew_does_not_hide_temperature_or_imply_saturation(self):
        temperature = 17.5
        for suffix, displayed_temperature in (("C", 17.5), ("F", 63.5)):
            for dew in (None, float("nan"), float("inf"), "12", True, 999, temperature + 1):
                with self.subTest(suffix=suffix, dew=dew):
                    self.assertEqual(chart.station_temperature_text(
                        {"temp_c": temperature, "dew_c": dew}, suffix), f"{displayed_temperature:.1f}{suffix}/—")
            self.assertEqual(chart.station_temperature_text({"temp_c": None, "dew_c": 12}, suffix), "—/—")

    def test_existing_history_is_upgraded_once_without_changing_observations(self):
        previous_cwd = Path.cwd()
        original_stations = copy.deepcopy(self.stations)
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                Path("snapshots").mkdir()
                for path in self.snapshot["charts"].values():
                    Path(path).write_text("old temperature-only chart")
                chart.HISTORY_PATH.write_text(json.dumps({"snapshots": [self.snapshot]}))

                def history():
                    return chart.parse_history_payload(chart.HISTORY_PATH.read_text()), "local"

                svgs = chart.build_snapshot_svgs(self.snapshot)
                with patch.object(chart, "load_station_history", side_effect=history), \
                     patch.object(chart, "history_continuity_required", return_value=True), \
                     patch.object(chart, "build_snapshot_svgs", wraps=chart.build_snapshot_svgs) as rebuild:
                    for minute in (5, 10):
                        chart.write_station_history(
                            list(self.stations.values()), chart.parse_iso_utc(f"2026-09-23T20:{minute:02d}:00Z"),
                            "test.01t", "2026-09-23T19:55:00Z", "live",
                            [(200, 23), (400, 22), (600, 21)], *svgs)
                    self.assertEqual(rebuild.call_count, 1)
                history = json.loads(chart.HISTORY_PATH.read_text())["snapshots"]
                self.assertEqual(len(history), 3)
                self.assertEqual(history[0]["stations"], original_stations)
                for snapshot in history:
                    self.assertEqual(snapshot["charts"]["render_version"], chart.CHART_RENDER_VERSION)
                    for unit in ("metric", "imperial"):
                        svg = Path(snapshot["charts"][f"{unit}_svg"]).read_text()
                        self.assertIn("Stations (temperature/dew-point spread)", svg)
                        self.assertIn("/+", svg)
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
