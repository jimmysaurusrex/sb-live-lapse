import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import replot_recent60_sba as chart


NOW = datetime(2026, 10, 3, 0, 5, tzinfo=timezone.utc)
TABLE = (Path(__file__).parent / "fixtures/mesowest_783SE.html").read_text()
MADIS = '<mesonet><record var="V-T" shef_id="783SE" elev="1161.5928" ObTime="2026-10-03T00:00" provider="SCE" data_value="300.15" /></mesonet>'


class StationReplacementTests(unittest.TestCase):
    def test_madis_queries_and_accepts_only_the_new_station(self):
        with patch.object(chart, "fetch_feed_text", return_value=MADIS) as fetch:
            row = chart.fetch_station("783SE", NOW)
        self.assertEqual(parse_qs(urlsplit(fetch.call_args.args[0]).query)["stanam"], ["783SE"])
        self.assertEqual(row["name"], "La Cumbre")
        self.assertEqual(chart.station_display_id(row["id"]), "783SE")
        self.assertAlmostEqual(row["temp_c"], 27)
        self.assertAlmostEqual(row["elev_m"] * chart.FT_PER_M, 3811, places=2)
        self.assertEqual(row["temp_source"]["station_id"], "783SE")
        old = chart.parse_station_madis("783SE", MADIS.replace("783SE", "AV377"), NOW)
        self.assertIsNone(old["temp_c"])

    def test_old_cache_cannot_supply_the_replacement(self):
        old = dict(chart.blank_station_row("KC6OYN"), temp_c=35,
                   temp_ob_time="2026-10-03T00:00")
        self.assertEqual(chart.parse_state_payload(json.dumps({"stations": {"KC6OYN": old}})), {})

    def test_historical_charts_keep_the_station_that_supplied_each_snapshot(self):
        for station_id, displayed, elevation in (("KC6OYN", "KC60YN", 1201),
                                                 ("783SE", "783SE", 1161.5928)):
            with self.subTest(station_id=station_id):
                row = dict(chart.blank_station_row(station_id), temp_c=27, dew_c=3.6,
                           elev_m=elevation, temp_ob_time="2026-10-03T00:00")
                snapshot = {"run_at": "2026-10-03T00:05:00Z", "stations": {station_id: row},
                            "rass": {"points_100m_c": [[100, 25], [800, 20], [1500, 15]],
                                     "ob_time_utc": "2026-10-03T00:00", "source": "live"}}
                rows = chart.snapshot_to_station_rows(snapshot)
                self.assertEqual(rows[0]["id"], station_id)
                self.assertEqual(rows[0]["elev_m"], elevation)
                for svg in chart.build_snapshot_svgs(snapshot):
                    self.assertIn(f"La Cumbre ({displayed})", svg)
                    self.assertNotIn("La Cumbre (783SE)" if station_id == "KC6OYN"
                                     else "La Cumbre (KC60YN)", svg)

    def test_refresh_uses_sce_fallback_and_then_its_own_cache(self):
        class Clock(datetime):
            current = NOW

            @classmethod
            def now(cls, tz=None):
                return cls.current

        requests = []

        def feed(url, timeout):
            requests.append(url)
            if Clock.current != NOW:
                raise TimeoutError("feed outage")
            query = parse_qs(urlsplit(url).query)
            if url.startswith(chart.MADIS_BASE):
                station_id = query["stanam"][0]
                if station_id == "783SE":
                    return "<mesonet/>"
                return MADIS.replace("783SE", station_id).replace("1161.5928", str(chart.MESOWEST_ELEV_M[station_id]))
            if url == chart.mesowest_station_url("783SE"):
                return TABLE
            raise AssertionError(f"unexpected station feed: {url}")

        def cache():
            return chart.parse_state_payload(chart.STATE_PATH.read_text()) if chart.STATE_PATH.exists() else {}

        def history():
            return (chart.parse_history_payload(chart.HISTORY_PATH.read_text()), "local") if chart.HISTORY_PATH.exists() else ([], "none")

        previous_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                with patch.object(chart, "datetime", Clock), \
                     patch.object(chart, "fetch_feed_text", side_effect=feed), \
                     patch.object(chart, "load_rass_with_fallback", return_value=("test.01t", "2026-10-03T00:00", [(100, 25), (800, 20), (1500, 15)], "live")), \
                     patch.object(chart, "load_last_good_state", side_effect=cache), \
                     patch.object(chart, "load_station_history", side_effect=history), \
                     patch.object(chart, "history_continuity_required", return_value=False), \
                     redirect_stdout(io.StringIO()), self.assertLogs(level="WARNING"):
                    for run in range(2):
                        Clock.current = NOW + timedelta(minutes=5 * run)
                        chart.main()
                        stations = json.loads(chart.STATE_PATH.read_text())["stations"]
                        self.assertEqual(len(stations), 7)
                        self.assertNotIn("KC6OYN", stations)
                        row = stations["783SE"]
                        self.assertEqual(row["temp_c"], 27)
                        self.assertEqual(row["dew_c"], 3.6)
                        self.assertEqual(row["wind_spd_mps"], 4.7)
                        self.assertEqual(row["wind_gust_mps"], 6.2)
                        self.assertEqual(row["wind_dir"], 225)
                        self.assertEqual(row["temp_ob_time"], "2026-10-03T00:00")
                        self.assertEqual(row["temp_source"]["service"], "MesoWest")
                        self.assertEqual(row["temp_source"]["station_id"], "783SE")
                        for svg in (chart.CHART_METRIC_PATH, chart.CHART_IMPERIAL_PATH):
                            self.assertIn("La Cumbre (783SE)", svg.read_text())
                            self.assertNotIn("KC60YN", svg.read_text())
                        self.assertIn("La Cumbre (783SE) 3811ft", chart.CHART_IMPERIAL_PATH.read_text())
                        if run == 0:
                            self.assertEqual(len(requests), 8)  # Seven MADIS, one MesoWest.
                    self.assertIn("(last-good)", row["provider"])
                    for snapshot in json.loads(chart.HISTORY_PATH.read_text())["snapshots"]:
                        self.assertIn("783SE", snapshot["stations"])
                        self.assertNotIn("KC6OYN", snapshot["stations"])
                    self.assertFalse(any("findu" in url or "AV377" in url or "KC6OYN" in url for url in requests))
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
