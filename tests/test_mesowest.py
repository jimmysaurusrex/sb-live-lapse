import io
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import replot_recent60_sba as chart


NOW = datetime(2026, 9, 19, 15, 15, tzinfo=timezone.utc)
FIXTURES = Path(__file__).parent / "fixtures"


def fixture(station_id):
    return (FIXTURES / f"mesowest_{station_id}.html").read_text()


class MesoWestTests(unittest.TestCase):
    def test_real_sce_raws_and_airport_tables(self):
        # Captured from public HTTPS tables during the 2026-09-19 MADIS outage.
        expected = {
            "SE068": (16.7, 6.3, "15:00", 1.2, 67.5),
            "SE234": (17.7, 10.9, "15:00", 2.5, 67.5),
            "MTIC1": (17.2, 12.7, "14:47", 1.8, 112.5),
            "MPWC1": (18.3, 12.5, "15:06", 0.4, 202.5),
            "421SE": (16.9, 16.8, "15:00", 0.3, 225.0),
            "KSBA": (19.0, 16.0, "15:00", 3.1, 22.5),
        }
        for station_id, (temp, dew, time, speed, direction) in expected.items():
            with self.subTest(station_id=station_id):
                row = chart.parse_station_mesowest(station_id, fixture(station_id), NOW)
                self.assertEqual(row["temp_c"], temp)
                self.assertEqual(row["dew_c"], dew)
                self.assertEqual(row["temp_ob_time"], f"2026-09-19T{time}")
                self.assertEqual(row["wind_spd_mps"], speed)
                self.assertEqual(row["wind_dir"], direction)
                self.assertEqual(row["elev_m"], chart.MESOWEST_ELEV_M[station_id])
                self.assertEqual(row["temp_source"]["service"], "MesoWest")
                self.assertEqual(row["temp_source"]["station_id"], station_id)
                self.assertEqual(row["temp_source"]["transport"], "https")

    def test_bad_identity_units_timezone_or_markup_are_rejected(self):
        raw = fixture("SE068")
        for bad in (raw.replace("SE068", "OTHER"), raw.replace("GMT", "PDT"),
                    raw.replace("Temperature<br>&#176; C", "Temperature<br>&#176; F"),
                    raw.replace("Tabular Listing", "Unavailable"),
                    "<html>maintenance</html>", "x" * (chart.FEED_MAX_BYTES + 1)):
            with self.subTest(raw=bad[:80]), self.assertRaises(ValueError):
                chart.parse_station_mesowest("SE068", bad, NOW)

    def test_missing_or_invalid_newest_temp_uses_older_valid_row(self):
        for bad in ("", "NaN", "inf", "-9999", "9999", "bad"):
            raw = fixture("SE068").replace('>   16.7</td>', f'>{bad}</td>')
            row = chart.parse_station_mesowest("SE068", raw, NOW)
            self.assertEqual(row["temp_ob_time"], "2026-09-19T14:50")
            self.assertEqual(row["temp_c"], 16.8)

    def test_missing_optional_values_are_not_borrowed_from_another_row(self):
        raw = (fixture("SE068").replace('>    6.3</td>', '>99</td>')
               .replace('>1.2</td>', '>-1</td>').replace('>3.3</td>', '>NaN</td>')
               .replace('>ENE</td>', '>VRB</td>'))
        row = chart.parse_station_mesowest("SE068", raw, NOW)
        self.assertEqual(row["temp_c"], 16.7)
        for key in ("dew_c", "wind_dir", "wind_spd_mps", "wind_gust_mps", "wind_ob_time"):
            self.assertIsNone(row[key], key)

    def test_nonmetric_wind_columns_are_not_misinterpreted(self):
        raw = fixture("SE068").replace("m/s", "mph")
        row = chart.parse_station_mesowest("SE068", raw, NOW)
        self.assertIsNone(row["wind_spd_mps"])
        self.assertIsNone(row["wind_gust_mps"])

    def test_stale_and_future_observations_never_become_current(self):
        raw = fixture("SE068")
        for now in (NOW + timedelta(hours=2), NOW - timedelta(hours=1)):
            self.assertIsNone(chart.parse_station_mesowest("SE068", raw, now)["temp_c"])
        row = chart.parse_station_mesowest("SE068", raw, NOW - timedelta(minutes=20))
        self.assertEqual(row["temp_ob_time"], "2026-09-19T14:50")

    def test_midnight_uses_previous_calendar_day_for_older_reading(self):
        raw = (fixture("SE068").replace("15:00", "0:10")
               .replace("14:50", "23:50").replace("14:40", "23:40")
               .replace('>   16.7</td>', '></td>'))
        now = datetime(2026, 9, 19, 0, 15, tzinfo=timezone.utc)
        row = chart.parse_station_mesowest("SE068", raw, now)
        self.assertEqual(row["temp_ob_time"], "2026-09-18T23:50")
        self.assertEqual(row["temp_c"], 16.8)

    def test_empty_madis_response_is_logged(self):
        with patch.object(chart, "fetch_feed_text", return_value="<mesonet/>"), self.assertLogs(level="WARNING") as logs:
            row = chart.fetch_station("SE068", NOW)
        self.assertIsNone(row["temp_c"])
        self.assertIn("no usable recent temperature", logs.output[0])

    def test_findu_only_receives_actual_cwop_callsigns(self):
        for station_id in chart.STATIONS:
            self.assertEqual(chart.should_try_cwop(chart.blank_station_row(station_id)), station_id == "KC6OYN")

    def test_fetch_failure_is_isolated_and_remains_https(self):
        with patch.object(chart, "fetch_feed_text", side_effect=TimeoutError("timeout")) as fetch, self.assertLogs(level="WARNING"):
            row = chart.fetch_station_mesowest("SE068", NOW)
        self.assertIsNone(row["temp_c"])
        fetch.assert_called_once_with(chart.mesowest_station_url("SE068"), timeout=22)
        self.assertTrue(fetch.call_args.args[0].startswith("https://"))

    def test_full_refresh_restores_seven_stations_and_preserves_cache_provenance(self):
        class Clock(datetime):
            current = NOW

            @classmethod
            def now(cls, tz=None):
                return cls.current

        requests = []

        def feed(url, timeout):
            requests.append(url)
            query = parse_qs(urlsplit(url).query)
            if Clock.current == NOW + timedelta(minutes=10):
                self.assertTrue(url.startswith(chart.MADIS_BASE))
                station_id = query["stanam"][0]
                return f'<mesonet><record var="V-T" shef_id="{station_id}" elev="493.5" ObTime="2026-09-19T15:10" provider="test" data_value="290.15" /></mesonet>'
            if Clock.current != NOW:
                raise TimeoutError("total outage")
            if url.startswith(chart.MADIS_BASE):
                if query["stanam"] == ["AV377"]:
                    return '<mesonet><record var="V-T" shef_id="AV377" elev="820.96" ObTime="2026-09-19T14:57" provider="APRSWXNET" data_value="289.261108" /></mesonet>'
                return "<mesonet/>"
            if url.startswith(chart.MESOWEST_BASE):
                return fixture(query["stn"][0])
            raise AssertionError(f"unexpected fallback {url}")

        def cache():
            return chart.parse_state_payload(chart.STATE_PATH.read_text()) if chart.STATE_PATH.exists() else {}

        def history():
            return (chart.parse_history_payload(chart.HISTORY_PATH.read_text()), "local") if chart.HISTORY_PATH.exists() else ([], "none")

        previous_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                with patch.object(chart, "datetime", Clock), \
                     patch.object(chart, "fetch_feed_text", side_effect=feed), \
                     patch.object(chart, "load_rass_with_fallback", return_value=("test.01t", "2026-09-19T15:00", [(100, 20), (1500, 10)], "live")), \
                     patch.object(chart, "load_last_good_state", side_effect=cache), \
                     patch.object(chart, "load_station_history", side_effect=history), \
                     patch.object(chart, "history_continuity_required", return_value=False), \
                     redirect_stdout(io.StringIO()), self.assertLogs(level="WARNING"):
                    chart.main()
                    self.assertEqual(len(requests), 13)  # 7 MADIS, 6 MesoWest, no findU.
                    state = json.loads(chart.STATE_PATH.read_text())["stations"]
                    for station_id in chart.MESOWEST_ELEV_M:
                        self.assertEqual(state[station_id]["temp_source"]["service"], "MesoWest")
                    for path in (chart.CHART_METRIC_PATH, chart.CHART_IMPERIAL_PATH):
                        root = ET.fromstring(path.read_text())
                        self.assertEqual(len(root.findall('.//{http://www.w3.org/2000/svg}rect[@class="station"]')), 7)
                    self.assertNotIn("FALSE", chart.CSV_PATH.read_text())
                    Clock.current += timedelta(minutes=5)
                    chart.main()
                    cached = json.loads(chart.STATE_PATH.read_text())["stations"]
                    for station_id in chart.STATIONS:
                        self.assertEqual(cached[station_id]["temp_ob_time"], state[station_id]["temp_ob_time"])
                        self.assertEqual(cached[station_id]["temp_source"], state[station_id]["temp_source"])
                        self.assertIn("(last-good)", cached[station_id]["provider"])
                    # Once MADIS recovers, extra upstream requests stop.
                    Clock.current += timedelta(minutes=5)
                    requests.clear()
                    chart.main()
                    self.assertEqual(len(requests), 7)
                    recovered = json.loads(chart.STATE_PATH.read_text())["stations"]
                    self.assertTrue(all(row["temp_source"]["service"] == "MADIS" for row in recovered.values()))
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
