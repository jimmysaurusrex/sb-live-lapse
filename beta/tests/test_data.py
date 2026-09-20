import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("beta_data", HERE.parent / "build_data.py")
feed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feed)
NOW = datetime(2026, 9, 20, 16, tzinfo=timezone.utc)


class CloudDataTests(unittest.TestCase):
    def setUp(self):
        self.metar = json.loads((HERE / "metar.json").read_text())
        self.camera = json.loads((HERE / "camera.json").read_text())

    def test_observed_base_converts_agl_to_msl(self):
        report = feed.parse_metar(json.dumps(self.metar), NOW)
        self.assertAlmostEqual(report["layers"][0]["base_msl_m"], 367.76)
        self.assertEqual(report["layers"][0]["cover"], "OVC")
        self.assertEqual(report["observed_at"], "2026-09-20T14:53:00Z")

    def test_missing_cloud_layers_does_not_mean_clear(self):
        self.metar[0].update(clouds=None, cover=None, rawOb="METAR KSBA")
        self.assertEqual(feed.parse_metar(json.dumps(self.metar), NOW)["sky"], "unknown")

    def test_clear_and_obscured_are_distinct(self):
        self.metar[0].update(clouds=[], cover="CLR", rawOb="METAR KSBA CLR")
        self.assertEqual(feed.parse_metar(json.dumps(self.metar), NOW)["sky"], "clear")
        self.metar[0].update(cover="VV", rawOb="METAR KSBA VV002")
        report = feed.parse_metar(json.dumps(self.metar), NOW)
        self.assertEqual(report["sky"], "obscured")
        self.assertEqual(report["layers"], [])

    def test_wrong_station_and_future_report_rejected(self):
        for changes in ({"icaoId": "KLAX"}, {"obsTime": NOW.timestamp() + 60}):
            report = {**self.metar[0], **changes}
            with self.assertRaises(ValueError):
                feed.parse_metar(json.dumps([report]), NOW)

    def test_invalid_height_cannot_create_cloud_marker(self):
        self.metar[0]["clouds"] = [{"cover": "OVC", "base": -1}, {"cover": "BKN", "base": True}]
        self.assertEqual(feed.parse_metar(json.dumps(self.metar), NOW)["layers"], [])

    def test_satellite_uses_latest_dated_nonfuture_image(self):
        names = ["20262631440", "20262631550", "20262631700"]
        raw = ''.join(f'<a href="{n}_GOES18-ABI-lox-GEOCOLOR-600x600.jpg">' for n in names)
        report = feed.parse_satellite(raw, NOW)
        self.assertEqual(report["observed_at"], "2026-09-20T15:50:00Z")
        self.assertIn("20262631550_", report["image_url"])

    def test_undated_satellite_is_not_treated_as_current(self):
        with self.assertRaises(ValueError):
            feed.parse_satellite('<a href="latest.jpg">', NOW)

    def test_camera_uses_linked_location_and_capture_timestamp(self):
        report = feed.parse_camera(json.dumps(self.camera), NOW)
        self.assertAlmostEqual(report["latitude"], 34.465286)
        self.assertAlmostEqual(report["longitude"], -119.678314)
        self.assertIn("/data/img/1986/2026/09/20/Gibraltar_2_", report["image_url"])
        self.assertEqual(report["name"], "Gibraltar 2")

    def test_camera_rejects_private_or_unexpected_image(self):
        for changes in ({"pv": 1}, {"img": "../../image.jpg"}, {"cn": "Other"},
                        {"img": f"Gibraltar_2_{int(NOW.timestamp()) + 60}_1.jpg"}):
            payload = copy.deepcopy(self.camera)
            payload["data"]["cams"]["data"][0].update(changes)
            with self.assertRaises(ValueError):
                feed.parse_camera(json.dumps(payload), NOW)

    def test_profile_read_is_read_only_and_excludes_future_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "station_history.json"
            path.write_text(json.dumps({"snapshots": [
                {"run_at": "2026-09-20T15:00:00", "stations": {"KSBA": {"temp_c": 19}}},
                {"run_at": "2026-09-20T17:00:00Z", "stations": {"KSBA": {"temp_c": 99}}}
            ]}))
            before = path.read_bytes(), path.stat().st_mtime_ns
            profile = feed.load_profile(directory, NOW)
            self.assertEqual(profile["stations"]["KSBA"]["temp_c"], 19)
            self.assertEqual(before, (path.read_bytes(), path.stat().st_mtime_ns))
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_source_failure_retains_observation_time_and_other_sources(self):
        previous = {"camera": {"observed_at": "2026-09-20T12:00:00Z", "name": "Gibraltar 2", "fetch_ok": True}}
        def fetch(url):
            if url == feed.METAR_URL:
                return json.dumps(self.metar)
            raise OSError("feed offline")
        with patch.object(feed, "fetch", side_effect=fetch), patch.object(feed, "load_profile", return_value={"stations": {"KSBA": {}}}):
            output = feed.build(previous=previous, now=NOW)
        self.assertTrue(output["airport"]["fetch_ok"])
        self.assertTrue(output["profile"]["fetch_ok"])
        self.assertFalse(output["camera"]["fetch_ok"])
        self.assertEqual(output["camera"]["observed_at"], previous["camera"]["observed_at"])
        self.assertEqual(output["satellite"], {"fetch_ok": False})
        self.assertTrue(previous["camera"]["fetch_ok"])


if __name__ == "__main__":
    unittest.main()
