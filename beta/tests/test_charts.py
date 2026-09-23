import copy
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

import replot_recent60_sba as primary_chart
from beta.build_charts import SVG, add_dew_points, build


class DewPointChartsTests(unittest.TestCase):
    def setUp(self):
        self.stations = {}
        for index, station_id in enumerate(primary_chart.STATIONS):
            row = primary_chart.blank_station_row(station_id)
            row.update(temp_c=20 + index, dew_c=12, elev_m=100 + index * 150,
                       temp_ob_time="2026-09-23T19:56:00Z", wind_ob_time="2026-09-23T19:56:00Z",
                       wind_dir=220, wind_spd_mps=2, wind_gust_mps=3)
            self.stations[station_id] = row
        # Matching temperatures must still get the correct station's dew point.
        self.stations["SE234"].update(temp_c=(86.3 - 32) * 5 / 9, dew_c=(53.9 - 32) * 5 / 9)
        self.stations["SE068"].update(temp_c=(86.3 - 32) * 5 / 9, dew_c=0)
        self.snapshot = {"run_at": "2026-09-23T20:00:00Z", "stations": self.stations,
                         "charts": {"metric_svg": "snapshots/20260923T2000Z_metric.svg",
                                    "imperial_svg": "snapshots/20260923T2000Z_imperial.svg"},
                         "rass": {"points_100m_c": [[200, 23], [400, 22], [600, 21]],
                                  "ob_time_utc": "2026-09-23T19:55:00Z", "source": "live"}}
        self.svgs = primary_chart.build_snapshot_svgs(self.snapshot)

    def graph_labels(self, svg):
        return {node[0].text: node[1].text.strip() for node in ET.fromstring(svg).findall(SVG + "text")
                if node.get("class") == "station-label"}

    def test_both_units_pair_the_correct_station_and_preserve_weather_geometry(self):
        original_stations = copy.deepcopy(self.stations)
        for index, unit in enumerate(("metric", "imperial")):
            original = self.svgs[index]
            result = add_dew_points(original, self.stations, unit)
            labels = self.graph_labels(result)
            self.assertEqual(labels["AntFarm"], "86.3F/+32.4F" if unit == "imperial" else "30.2C/+18.0C")
            self.assertEqual(labels["VOR"], "86.3F/+54.3F" if unit == "imperial" else "30.2C/+30.2C")
            rows = ["".join(node.itertext()) for node in ET.fromstring(result).findall(SVG + "text")
                    if node.get("class") == "legend-row"]
            for row in rows:
                name = row.split(" (")[0]
                self.assertIn(labels[name], row)
            def geometry(svg):
                root = ET.fromstring(svg)
                return [(node.tag, node.attrib) for node in root if node.tag != SVG + "text"]
            self.assertEqual(geometry(original), geometry(result))
        self.assertEqual(self.stations, original_stations)

    def test_spread_examples_leading_zero_and_saturation_on_graph_and_table(self):
        cases = [
            ("imperial", (63.5 - 32) * 5 / 9, (62.2 - 32) * 5 / 9, "63.5F/+1.3F"),
            ("imperial", (63.5 - 32) * 5 / 9, (63.0 - 32) * 5 / 9, "63.5F/+0.5F"),
            ("imperial", 17.5, 17.5, "63.5F/saturated"),
            ("metric", -5, -5.5, "-5.0C/+0.5C"),
            ("metric", 0, 0, "0.0C/saturated"),
            ("metric", 10, 9.96, "10.0C/saturated"),
            ("imperial", 10, 9.96, "50.0F/+0.1F"),
        ]
        for unit, temperature, dew, expected in cases:
            with self.subTest(unit=unit, temperature=temperature, dew=dew):
                self.stations["SE234"].update(temp_c=temperature, dew_c=dew)
                source = primary_chart.build_snapshot_svgs(self.snapshot)[unit == "imperial"]
                result = add_dew_points(source, self.stations, unit)
                self.assertEqual(self.graph_labels(result)["AntFarm"], expected)
                self.assertIn(f" - {expected},", result)

    def test_missing_and_invalid_dew_points_stay_missing_without_losing_temperature(self):
        for bad_dew in (None, float("nan"), float("inf"), "12", 99, 40):
            self.stations["SE234"]["dew_c"] = bad_dew
            result = add_dew_points(self.svgs[1], self.stations, "imperial")
            self.assertEqual(self.graph_labels(result)["AntFarm"], "86.3F/—")
            self.assertIn(" - 86.3F/—,", result)

    def test_already_promoted_primary_labels_are_idempotent(self):
        temperature = self.stations["SE234"]["temp_c"]
        for unit, source in zip(("metric", "imperial"), self.svgs):
            self.stations["SE234"]["temp_c"] = temperature
            for dew in (12, self.stations["SE234"]["temp_c"], None):
                self.stations["SE234"]["dew_c"] = dew
                first = add_dew_points(source, self.stations, unit)
                self.assertEqual(add_dew_points(first, self.stations, unit), first)
            self.stations["SE234"]["temp_c"] = None
            missing = add_dew_points(source, self.stations, unit)
            self.assertEqual(add_dew_points(missing, self.stations, unit), missing)

    def test_longer_label_and_its_lapse_value_flip_together_at_right_edge(self):
        root = ET.fromstring(self.svgs[1])
        elements = list(root)
        label = next(node for node in elements if node.get("class") == "station-label")
        label.set("x", "1160.00")
        label.set("text-anchor", "start")
        deviation = elements[elements.index(label) + 1]
        deviation.set("x", "1160.00")
        deviation.set("text-anchor", "start")
        result = ET.fromstring(add_dew_points(ET.tostring(root, encoding="unicode"), self.stations, "imperial"))
        labels = list(result)
        moved = next(node for node in labels if node.get("class") == "station-label")
        paired = labels[labels.index(moved) + 1]
        self.assertEqual(moved.get("text-anchor"), "end")
        self.assertEqual(moved.get("x"), "1148.00")
        self.assertEqual(moved.get("x"), paired.get("x"))
        self.assertEqual(moved.get("text-anchor"), paired.get("text-anchor"))

    def test_build_preserves_primary_and_adds_pairs_to_latest_and_history(self):
        with tempfile.TemporaryDirectory() as directory:
            primary, output = Path(directory) / "primary", Path(directory) / "beta"
            (primary / "snapshots").mkdir(parents=True)
            state = {"generated_at": self.snapshot["run_at"], "stations": self.stations}
            (primary / "station_state.json").write_text(json.dumps(state))
            (primary / "station_history.json").write_text(json.dumps({"snapshots": [self.snapshot], "snapshot_count": 1}))
            for unit, svg in zip(("metric", "imperial"), self.svgs):
                (primary / f"sba_wwtemp_chart_{unit}.svg").write_text(svg)
                (primary / self.snapshot["charts"][f"{unit}_svg"]).write_text(svg)
            before = {str(path): path.read_bytes() for path in primary.rglob("*") if path.is_file()}
            result = build(primary, output)
            self.assertEqual(result["snapshots"], 1)
            for path in [output / "sba_wwtemp_chart_imperial.svg", output / self.snapshot["charts"]["imperial_svg"]]:
                self.assertIn("86.3F/+32.4F", path.read_text())
            self.assertEqual(json.loads((output / "station_history.json").read_text())["snapshots"][0]["charts"], self.snapshot["charts"])
            self.assertEqual(before, {str(path): path.read_bytes() for path in primary.rglob("*") if path.is_file()})
            with self.assertRaises(ValueError):
                build(primary, primary)


if __name__ == "__main__":
    unittest.main()
