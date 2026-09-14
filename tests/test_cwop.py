import ssl
import unittest
from datetime import datetime, timezone
from unittest.mock import call, patch
from urllib.error import URLError

import replot_recent60_sba as chart


REPORTS = """<station>
  <call>KC6OYN</call>
  <weatherReport>
    <timeReceived>20260914173805</timeReceived>
    <temperature>75</temperature><humidity>41</humidity>
    <windSpeed>0</windSpeed><windDirection>324</windDirection><windGust>2</windGust>
  </weatherReport>
  <weatherReport>
    <timeReceived>20260914173003</timeReceived>
    <temperature>74</temperature><humidity>39</humidity>
    <windSpeed>1</windSpeed><windDirection>326</windDirection><windGust>3</windGust>
  </weatherReport>
</station>"""


class CwopTests(unittest.TestCase):
    def test_https_certificate_failure_recovers_fresh_station(self):
        error = URLError(ssl.SSLCertVerificationError("certificate verify failed"))
        with patch.object(chart, "fetch_text", side_effect=[error, REPORTS]) as fetch:
            with self.assertLogs(level="WARNING") as logs:
                cwop = chart.fetch_station_cwop("KC6OYN")
        self.assertEqual(fetch.call_args_list, [
            call("https://www.findu.com/cgi-bin/wxxml.cgi?call=KC6OYN&last=2", timeout=18),
            call("http://www.findu.com/cgi-bin/wxxml.cgi?call=KC6OYN&last=2", timeout=18),
        ])
        self.assertTrue(any("certificate verify failed" in line for line in logs.output))
        self.assertTrue(any("using public HTTP fallback" in line for line in logs.output))
        row = chart.merge_cwop_if_needed(chart.blank_station_row("KC6OYN"), cwop)
        chart.update_age_and_recency(row, datetime(2026, 9, 14, 17, 40, tzinfo=timezone.utc))
        self.assertTrue(row["recent"])
        self.assertAlmostEqual(row["temp_c"], 23.8888888889)
        self.assertEqual(row["provider"], "CWOP-findU")
        self.assertEqual(row["elev_m"], 1201)

    def test_healthy_https_never_uses_http(self):
        with patch.object(chart, "fetch_text", return_value=REPORTS) as fetch:
            row = chart.fetch_station_cwop("KC6OYN")
        self.assertEqual(fetch.call_count, 1)
        self.assertTrue(fetch.call_args.args[0].startswith("https://"))
        self.assertEqual(row["temp_ob_time"], "2026-09-14T17:38")
        self.assertEqual(row["wind_ob_time"], "2026-09-14T17:38")
        self.assertEqual(row["wind_spd_mps"], 0)
        self.assertAlmostEqual(row["wind_gust_mps"], 2 / chart.MS_TO_MPH)
        self.assertEqual(row["wind_dir"], 324)
        self.assertAlmostEqual(row["dew_c"], 9.835, places=2)

    def test_unusable_https_response_tries_fallback(self):
        for response in ("<html>Unavailable</html>", "not XML", "<station><call>KC6OYN</call></station>"):
            with self.subTest(response=response):
                with patch.object(chart, "fetch_text", side_effect=[response, REPORTS]):
                    with self.assertLogs(level="WARNING"):
                        row = chart.fetch_station_cwop("KC6OYN")
                self.assertIsNotNone(row["temp_c"])

    def test_both_endpoints_fail_without_aborting_other_stations(self):
        with patch.object(chart, "fetch_text", side_effect=TimeoutError("timed out")) as fetch:
            with self.assertLogs(level="WARNING") as logs:
                row = chart.fetch_station_cwop("KC6OYN")
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(len(logs.output), 2)
        self.assertIsNone(row["temp_c"])
        self.assertIsNone(row["temp_ob_time"])
        self.assertEqual(row["elev_m"], 1201)

    def test_wrong_station_is_rejected(self):
        with patch.object(chart, "fetch_text", return_value=REPORTS.replace("KC6OYN", "OTHER")):
            with self.assertLogs(level="WARNING"):
                row = chart.fetch_station_cwop("KC6OYN")
        self.assertIsNone(row["temp_c"])

    def test_old_fallback_reading_is_not_marked_fresh(self):
        with patch.object(chart, "fetch_text", return_value=REPORTS):
            row = chart.fetch_station_cwop("KC6OYN")
        chart.update_age_and_recency(row, datetime(2026, 9, 14, 19, tzinfo=timezone.utc))
        self.assertFalse(row["recent"])
        self.assertEqual(row["temp_ob_time"], "2026-09-14T17:38")

    def test_recent_madis_still_takes_precedence(self):
        madis = dict(chart.blank_station_row("KC6OYN"), temp_c=21, recent=True)
        cwop = chart.parse_station_cwop("KC6OYN", REPORTS)
        self.assertIs(chart.merge_cwop_if_needed(madis, cwop), madis)


if __name__ == "__main__":
    unittest.main()
