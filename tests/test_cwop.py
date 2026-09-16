import io
import json
import os
import ssl
import tempfile
import unittest
import urllib.request
import xml.etree.ElementTree as ET
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit

import replot_recent60_sba as chart


NOW = datetime(2026, 9, 16, 21, 40, tzinfo=timezone.utc)
REPORTS = """<station>
  <call>KC6OYN</call>
  <weatherReport>
    <timeReceived>20260916213805</timeReceived>
    <temperature>75</temperature><humidity>41</humidity>
    <windSpeed>0</windSpeed><windDirection>324</windDirection><windGust>2</windGust>
  </weatherReport>
  <weatherReport>
    <timeReceived>20260916213003</timeReceived>
    <temperature>74</temperature><humidity>39</humidity>
    <windSpeed>1</windSpeed><windDirection>326</windDirection><windGust>3</windGust>
  </weatherReport>
</station>"""

# Representative NOAA AV377 response, verified over HTTPS on 2026-09-16.
MADIS = """<mesonet>
 <record var="V-TD" shef_id="AV377" elev="820.96" ObTime="2026-09-16T21:26" provider="APRSWXNET" data_value="286.355560" />
 <record var="V-T" shef_id="AV377" elev="820.96" ObTime="2026-09-16T21:26" provider="APRSWXNET" data_value="290.372223" />
 <record var="V-DD" shef_id="AV377" elev="820.96" ObTime="2026-09-16T21:26" provider="APRSWXNET" data_value="180.000000" />
 <record var="V-FF" shef_id="AV377" elev="820.96" ObTime="2026-09-16T21:26" provider="APRSWXNET" data_value="6.258560" />
 <record var="V-FFGUST" shef_id="AV377" elev="820.96" ObTime="2026-09-16T21:26" provider="APRSWXNET" data_value="7.599680" />
</mesonet>"""


class CwopTests(unittest.TestCase):
    def test_certificate_failure_never_downgrades_to_http(self):
        error = URLError(ssl.SSLCertVerificationError("certificate verify failed"))
        with patch.object(chart, "fetch_feed_text", side_effect=error) as fetch:
            with self.assertLogs(level="WARNING") as logs:
                row = chart.fetch_station_cwop("KC6OYN", NOW)
        fetch.assert_called_once_with(
            "https://www.findu.com/cgi-bin/wxxml.cgi?call=KC6OYN&last=2", timeout=18)
        self.assertTrue(any("certificate verify failed" in line for line in logs.output))
        self.assertIsNone(row["temp_c"])
        self.assertIsNone(row["temp_source"])
        self.assertEqual(row["elev_m"], 1201)

    def test_healthy_https_preserves_units_and_provenance(self):
        with patch.object(chart, "fetch_feed_text", return_value=REPORTS) as fetch:
            row = chart.fetch_station_cwop("KC6OYN", NOW)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(row["temp_ob_time"], "2026-09-16T21:38")
        self.assertEqual(row["wind_ob_time"], "2026-09-16T21:38")
        self.assertAlmostEqual(row["temp_c"], 23.8888888889)
        self.assertEqual(row["wind_spd_mps"], 0)
        self.assertAlmostEqual(row["wind_gust_mps"], 2 / chart.MS_TO_MPH)
        self.assertEqual(row["wind_dir"], 324)
        self.assertAlmostEqual(row["dew_c"], 9.835, places=2)
        self.assertEqual(row["temp_source"]["transport"], "https")
        self.assertEqual(row["provider"], "CWOP-findU (KC6OYN; HTTPS)")

    def test_bad_responses_fail_without_another_endpoint(self):
        responses = ["<html>Unavailable</html>", "not XML",
                     "<station><call>KC6OYN</call></station>",
                     REPORTS.replace("KC6OYN", "OTHER")]
        for response in responses:
            with self.subTest(response=response):
                with patch.object(chart, "fetch_feed_text", return_value=response) as fetch:
                    with self.assertLogs(level="WARNING"):
                        row = chart.fetch_station_cwop("KC6OYN", NOW)
                self.assertIsNone(row["temp_c"])
                self.assertEqual(fetch.call_count, 1)

    def test_invalid_newest_temperature_does_not_hide_valid_older_report(self):
        for value in ("NaN", "inf", "-inf", "9999", "-9999", "", "oops"):
            with self.subTest(value=value):
                raw = REPORTS.replace("<temperature>75</temperature>", f"<temperature>{value}</temperature>")
                row = chart.parse_station_cwop("KC6OYN", raw, NOW)
                self.assertEqual(row["temp_ob_time"], "2026-09-16T21:30")
                self.assertAlmostEqual(row["temp_c"], (74 - 32) * 5 / 9)

    def test_future_or_invalid_newest_timestamp_does_not_mask_good_report(self):
        for timestamp in ("20260916214100", "20990916213805", "not a time", "20261316213805"):
            with self.subTest(timestamp=timestamp):
                row = chart.parse_station_cwop("KC6OYN", REPORTS.replace("20260916213805", timestamp), NOW)
                self.assertEqual(row["temp_ob_time"], "2026-09-16T21:30")

    def test_old_readings_are_unusable(self):
        row = chart.parse_station_cwop("KC6OYN", REPORTS, NOW + timedelta(hours=2))
        self.assertIsNone(row["temp_c"])

    def test_invalid_optional_values_are_omitted(self):
        raw = (REPORTS.replace("<humidity>41", "<humidity>101")
               .replace("<windDirection>324", "<windDirection>361")
               .replace("<windSpeed>0", "<windSpeed>-1")
               .replace("<windGust>2", "<windGust>inf"))
        row = chart.parse_station_cwop("KC6OYN", raw, NOW)
        self.assertIsNotNone(row["temp_c"])
        for key in ("dew_c", "wind_dir", "wind_spd_mps", "wind_gust_mps", "wind_ob_time"):
            self.assertIsNone(row[key], key)

    def test_recent_madis_takes_precedence(self):
        madis = chart.parse_station_madis("KC6OYN", MADIS, NOW)
        chart.update_age_and_recency(madis, NOW)
        cwop = chart.parse_station_cwop("KC6OYN", REPORTS, NOW)
        self.assertFalse(chart.should_try_cwop(madis))
        self.assertIs(chart.merge_cwop_if_needed(madis, cwop), madis)

    def test_cwop_replaces_stale_madis_and_preserves_provenance(self):
        madis = dict(chart.blank_station_row("KC6OYN"), temp_c=21, dew_c=20,
                     temp_ob_time="2026-09-16T19:00", recent=False)
        with patch.object(chart, "fetch_feed_text", return_value=REPORTS.replace("<humidity>41", "<humidity>101")):
            cwop = chart.fetch_station_cwop("KC6OYN", NOW)
        merged = chart.merge_cwop_if_needed(madis, cwop)
        self.assertEqual(merged["temp_source"], cwop["temp_source"])
        self.assertIsNone(merged["dew_c"])


class MadisTests(unittest.TestCase):
    def test_assigned_id_yields_fresh_kc6oyn_without_findu(self):
        with patch.object(chart, "fetch_feed_text", return_value=MADIS) as fetch:
            row = chart.fetch_station("KC6OYN", NOW)
        url = fetch.call_args.args[0]
        self.assertEqual(urlsplit(url).scheme, "https")
        self.assertEqual(parse_qs(urlsplit(url).query)["stanam"], ["AV377"])
        self.assertEqual(row["id"], "KC6OYN")
        self.assertEqual(row["name"], "La Cumbre")
        self.assertEqual(chart.station_display_id(row["id"]), "KC60YN")
        self.assertAlmostEqual(row["temp_c"], 17.222223)
        self.assertAlmostEqual(row["wind_spd_mps"], 6.258560)
        self.assertEqual(row["elev_m"], 1201)
        self.assertEqual(row["temp_source"]["reported_elev_m"], 820.96)
        self.assertEqual(row["provider"], "MADIS-APRSWXNET (AV377; HTTPS)")
        chart.update_age_and_recency(row, NOW)
        self.assertTrue(row["recent"])
        self.assertFalse(chart.should_try_cwop(row))

    def test_other_station_ids_are_not_remapped(self):
        for station_id in ("KSBA", "SE068"):
            with self.subTest(station_id=station_id):
                self.assertEqual(parse_qs(urlsplit(chart.madis_station_url(station_id)).query)["stanam"], [station_id])
                row = chart.parse_station_madis(station_id, MADIS.replace("AV377", station_id), NOW)
                self.assertIsNotNone(row["temp_c"])
                self.assertEqual(row["elev_m"], 820.96)

    def test_wrong_station_and_stale_or_future_records_are_rejected(self):
        for raw in (MADIS.replace("AV377", "OTHER"),
                    MADIS.replace("2026-09-16T21:26", "2026-09-16T20:39"),
                    MADIS.replace("2026-09-16T21:26", "2026-09-16T21:41"),
                    MADIS.replace("2026-09-16T21:26", "invalid")):
            with self.subTest(raw=raw):
                row = chart.parse_station_madis("KC6OYN", raw, NOW)
                self.assertIsNone(row["temp_c"])
                self.assertIsNone(row["wind_spd_mps"])

    def test_invalid_temperature_values_are_rejected(self):
        for value in ("NaN", "inf", "-inf", "9999", "0", "", "oops"):
            with self.subTest(value=value):
                row = chart.parse_station_madis("KC6OYN", MADIS.replace("290.372223", value), NOW)
                self.assertIsNone(row["temp_c"])
                self.assertIsNone(row["temp_source"])

    def test_selects_newest_usable_report_regardless_of_order(self):
        invalid = MADIS.replace("21:26", "21:39").replace("290.372223", "NaN")
        older = MADIS.replace("21:26", "21:20").replace("290.372223", "289.0")
        for parts in ((invalid, older, MADIS), (MADIS, older, invalid)):
            with self.subTest(parts=parts):
                raw = "<mesonet>" + "".join(p.removeprefix("<mesonet>").removesuffix("</mesonet>") for p in parts) + "</mesonet>"
                row = chart.parse_station_madis("KC6OYN", raw, NOW)
                self.assertEqual(row["temp_ob_time"], "2026-09-16T21:26")
                self.assertAlmostEqual(row["temp_c"], 17.222223)

    def test_invalid_optional_values_are_omitted(self):
        raw = (MADIS.replace("286.355560", "NaN").replace("180.000000", "361")
               .replace("6.258560", "-1").replace("7.599680", "9999"))
        row = chart.parse_station_madis("KC6OYN", raw, NOW)
        self.assertIsNotNone(row["temp_c"])
        for key in ("dew_c", "wind_dir", "wind_spd_mps", "wind_gust_mps", "wind_ob_time"):
            self.assertIsNone(row[key], key)

    def test_malformed_response_or_timeout_is_isolated(self):
        for result in ("not XML", "<html>unavailable</html>", TimeoutError("timed out")):
            with self.subTest(result=result):
                with patch.object(chart, "fetch_feed_text", side_effect=[result]):
                    with self.assertLogs(level="WARNING"):
                        row = chart.fetch_station("KC6OYN", NOW)
                self.assertIsNone(row["temp_c"])


class FeedTransportTests(unittest.TestCase):
    def test_http_is_rejected_before_opening(self):
        with patch.object(urllib.request, "build_opener") as build:
            with self.assertRaisesRegex(ValueError, "require HTTPS"):
                chart.fetch_feed_text("http://www.findu.com/cgi-bin/wxxml.cgi")
        build.assert_not_called()

    def test_redirects_cannot_downgrade_or_change_source(self):
        req = urllib.request.Request(chart.CWOP_XML_BASE)
        for target in ("http://www.findu.com/", "https://other.example/", chart.CWOP_XML_BASE):
            with self.subTest(target=target):
                with self.assertRaisesRegex(ValueError, "redirect refused"):
                    chart.FeedRedirectHandler().redirect_request(req, None, 302, "Found", {}, target)

    def test_default_opener_has_certificate_and_hostname_verification(self):
        # Exercise the actual opener construction, replacing only the network call.
        seen = []
        real_build = urllib.request.build_opener
        def capture(*handlers):
            opener = real_build(*handlers)
            seen.append(opener)
            opener.open = MagicMock(return_value=io.BytesIO(REPORTS.encode()))
            return opener
        with patch.object(urllib.request, "build_opener", side_effect=capture):
            self.assertEqual(chart.fetch_feed_text(chart.CWOP_XML_BASE), REPORTS)
        https = next(h for h in seen[0].handlers if isinstance(h, urllib.request.HTTPSHandler))
        # None delegates to http.client's default verified context.
        context = https._context or ssl.create_default_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)
        self.assertTrue(any(isinstance(h, chart.FeedRedirectHandler) for h in seen[0].handlers))

    def test_response_size_is_bounded_before_parsing(self):
        opener = MagicMock()
        response = opener.open.return_value.__enter__.return_value
        response.read.return_value = b"x" * (chart.FEED_MAX_BYTES + 1)
        with patch.object(urllib.request, "build_opener", return_value=opener):
            with self.assertRaisesRegex(ValueError, "too large"):
                chart.fetch_feed_text(chart.CWOP_XML_BASE)
        response.read.assert_called_once_with(chart.FEED_MAX_BYTES + 1)

    def test_xml_entities_and_oversized_inputs_are_rejected(self):
        for raw in ('<!DOCTYPE station [<!ENTITY x "75">]>' + REPORTS,
                    "x" * (chart.FEED_MAX_BYTES + 1)):
            with self.subTest(raw=raw[:50]):
                with self.assertRaises(ValueError):
                    chart.parse_station_cwop("KC6OYN", raw, NOW)
                with self.assertRaises(ValueError):
                    chart.parse_station_madis("KC6OYN", raw, NOW)


class ContinuityTests(unittest.TestCase):
    def test_slow_fetch_uses_receipt_time_for_validation(self):
        class Clock(datetime):
            current = NOW - timedelta(minutes=20)

            @classmethod
            def now(cls, tz=None):
                return cls.current

        def feed(url, timeout):
            Clock.current = NOW
            return MADIS if url.startswith(chart.MADIS_BASE) else REPORTS

        with patch.object(chart, "datetime", Clock), patch.object(chart, "fetch_feed_text", side_effect=feed):
            for fetch in (chart.fetch_station, chart.fetch_station_cwop):
                Clock.current = NOW - timedelta(minutes=20)
                row = fetch("KC6OYN")
                self.assertIsNotNone(row["temp_c"])
                chart.update_age_and_recency(row, Clock.current)
                self.assertTrue(row["recent"])

    def test_age_limits_include_zero_and_sixty_but_never_future(self):
        for age, recent, grace in ((-1, False, False), (0, True, True),
                                   (60, True, True), (61, False, True),
                                   (90, False, True), (91, False, False)):
            with self.subTest(age=age):
                timestamp = (NOW - timedelta(minutes=age)).isoformat()
                row = dict(chart.blank_station_row("KC6OYN"), temp_ob_time=timestamp)
                chart.update_age_and_recency(row, NOW)
                self.assertEqual(row["recent"], recent)
                self.assertEqual(chart.within_grace(timestamp, NOW), grace)

    def test_cache_keeps_observation_time_and_provenance(self):
        original = chart.parse_station_madis("KC6OYN", MADIS, NOW)
        saved = json.dumps({"stations": {"KC6OYN": chart.station_payload(original)}})
        cached = chart.parse_state_payload(saved)["KC6OYN"]
        restored = chart.apply_last_good_fallback(chart.blank_station_row("KC6OYN"), cached, NOW)
        self.assertEqual(restored["temp_source"], original["temp_source"])
        self.assertEqual(restored["temp_ob_time"], original["temp_ob_time"])
        self.assertEqual(restored["provider"], original["provider"] + " (last-good)")
        expired = chart.apply_last_good_fallback(chart.blank_station_row("KC6OYN"), cached, NOW + timedelta(hours=2))
        self.assertIsNone(expired["temp_c"])

    def test_legacy_cached_reading_is_not_relabelled_as_https(self):
        legacy = dict(chart.blank_station_row("KC6OYN"), temp_c=21,
                      temp_ob_time="2026-09-16T21:30", provider="CWOP-findU")
        restored = chart.apply_last_good_fallback(chart.blank_station_row("KC6OYN"), legacy, NOW)
        self.assertIsNone(restored["temp_source"])
        self.assertEqual(restored["provider"], "CWOP-findU (last-good)")

    def test_full_chart_refresh_survives_findu_failure_then_total_feed_outage(self):
        class Clock(datetime):
            current = NOW
            @classmethod
            def now(cls, tz=None):
                return cls.current

        def feed(url, timeout):
            if url.startswith(chart.MADIS_BASE) and "stanam=AV377" in url and Clock.current == NOW:
                return MADIS
            if url.startswith(chart.CWOP_XML_BASE):
                raise URLError(ssl.SSLCertVerificationError("findU certificate failed"))
            raise TimeoutError("MADIS unavailable")

        def history():
            if chart.HISTORY_PATH.exists():
                return chart.parse_history_payload(chart.HISTORY_PATH.read_text()), "local"
            return [], "none"

        def cache():
            return chart.parse_state_payload(chart.STATE_PATH.read_text()) if chart.STATE_PATH.exists() else {}

        previous_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                with patch.object(chart, "datetime", Clock), \
                     patch.object(chart, "STATIONS", ["KC6OYN", "KSBA"]), \
                     patch.object(chart, "fetch_feed_text", side_effect=feed) as fetch, \
                     patch.object(chart, "load_rass_with_fallback", return_value=("test.01t", "2026-09-16T21:00", [(100, 20), (1500, 10)], "live")), \
                     patch.object(chart, "load_station_history", side_effect=history), \
                     patch.object(chart, "load_last_good_state", side_effect=cache), \
                     patch.object(chart, "history_continuity_required", return_value=False), \
                     redirect_stdout(io.StringIO()), self.assertLogs(level="WARNING"):
                    for run in range(2):
                        Clock.current = NOW + timedelta(minutes=5 * run)
                        chart.main()
                        state = json.loads(chart.STATE_PATH.read_text())
                        row = state["stations"]["KC6OYN"]
                        self.assertEqual(row["temp_source"]["station_id"], "AV377")
                        self.assertEqual(row["temp_ob_time"], "2026-09-16T21:26")
                        self.assertEqual(state["generated_at"], Clock.current.strftime("%Y-%m-%dT%H:%M:%SZ"))
                        for path in (chart.CHART_METRIC_PATH, chart.CHART_IMPERIAL_PATH):
                            root = ET.fromstring(path.read_text())
                            markers = root.findall('.//{http://www.w3.org/2000/svg}rect[@class="station"]')
                            self.assertEqual(len(markers), 1)
                            self.assertIn("La Cumbre (KC60YN)", path.read_text())
                        self.assertIn("AV377; HTTPS", chart.CSV_PATH.read_text())
                    self.assertIn("(last-good)", row["provider"])
                    snapshots = json.loads(chart.HISTORY_PATH.read_text())["snapshots"]
                    self.assertEqual(len(snapshots), 2)
                    for snapshot in snapshots:
                        self.assertEqual(snapshot["stations"]["KC6OYN"]["temp_source"], row["temp_source"])
                    self.assertTrue(all(c.args[0].startswith("https://") for c in fetch.call_args_list))
                    kc_findu_calls = [c for c in fetch.call_args_list if "call=KC6OYN" in c.args[0]]
                    self.assertEqual(len(kc_findu_calls), 1)  # Only the second run needs findU.
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
