import importlib.util
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
import tempfile
import unittest

from PIL import Image

spec = importlib.util.spec_from_file_location("cameras", Path(__file__).parents[1] / "build_cameras.py")
cameras = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cameras)
NOW = datetime(2026, 9, 26, 2, tzinfo=timezone.utc)
STAMP = str(int(NOW.timestamp()) - 120)


def payload(key):
    camera_id, name, *_ = cameras.CAMERAS[key]
    return {"code": 1, "data": {camera_id: {"cur": {
        "cam_id": camera_id, "cam_name": name, "full": {"private": "0"},
        "cmlg_img_name": f"{name}_{STAMP}_1234_p.jpg", "cmlg_timestamp": STAMP,
        "cmlg_date": "2026-09-26", "cmlg_cam_azimuth": "163.33", "cmlg_cam_fov": "391.98",
    }}}}


class CameraTests(unittest.TestCase):
    def test_metadata_identity_privacy_timestamp_and_filename_validation(self):
        p = payload("gibraltar")
        result = cameras.parse_panorama(json.dumps(p), "gibraltar", NOW)
        self.assertEqual(result["stamp"], STAMP)
        self.assertIn("/1985/2026/09/26/Gibraltar_1_", result["source_image"])
        for field, value in [("cam_id", "1986"), ("cam_name", "Other"),
                             ("cmlg_img_name", "../../bad.jpg"), ("cmlg_cam_fov", "NaN"),
                             ("cmlg_cam_azimuth", "inf"), ("cmlg_date", "2026-09-25")]:
            invalid = payload("gibraltar")
            invalid["data"]["1985"]["cur"][field] = value
            with self.assertRaises(ValueError):
                cameras.parse_panorama(json.dumps(invalid), "gibraltar", NOW)
        p["data"]["1985"]["cur"]["full"]["private"] = "1"
        with self.assertRaises(ValueError):
            cameras.parse_panorama(json.dumps(p), "gibraltar", NOW)
        for now in (NOW - timedelta(hours=1), NOW + timedelta(days=2)):
            with self.assertRaises(ValueError):
                cameras.parse_panorama(json.dumps(payload("gibraltar")), "gibraltar", now)

    def test_bearing_crop_and_panorama_wrap_preserve_pixels(self):
        # A calibrated 392-degree source beginning at -32.66°: blue north,
        # yellow east, red south, green west. Overlapping edges must wrap once.
        source = Image.new("RGB", (3920, 360))
        colors = [(0, 0, 255), (255, 255, 0), (255, 0, 0), (0, 255, 0)]
        for x in range(source.width):
            bearing = (-32.66 + x / source.width * 391.98) % 360
            source.paste(colors[int((bearing + 45) % 360 // 90)], (x, 0, x + 1, source.height))
        for center, expected in [(180, colors[2]), (0, colors[0]), (270, colors[3])]:
            view = cameras.direction_view(source, 163.33, 391.98, center, 65.33, 600)
            self.assertEqual(view.getpixel((300, view.height // 2)), expected)
        north = cameras.direction_view(source, 163.33, 391.98, 0, 360, 1200)
        for x, expected in [(2, colors[2]), (300, colors[3]), (600, colors[0]), (900, colors[1]), (1197, colors[2])]:
            self.assertEqual(north.getpixel((x, north.height // 2)), expected)

        tvhill = cameras.direction_view(source, 163.33, 391.98, 30, 120, 1200)
        # 330° to 090° crosses north at one quarter of the image, not its center.
        for x, expected in [(2, colors[0]), (300, colors[0]), (900, colors[1]), (1197, colors[1])]:
            self.assertEqual(tvhill.getpixel((x, tvhill.height // 2)), expected)
        self.assertEqual(cameras.CAMERAS['tvhill'][2:4], (30, 120))
        self.assertNotIn('ortega', cameras.CAMERAS)

    def test_refresh_reuses_files_and_retains_each_last_good_camera_on_failure(self):
        raw = io.BytesIO()
        Image.new("RGB", (11520, 1080), "gray").save(raw, "JPEG")
        requests = []

        def reader(url, deadline, limit):
            requests.append(url)
            if url.endswith(".jpg"):
                return raw.getvalue()
            key = next(k for k, v in cameras.CAMERAS.items() if f"camId={v[0]}&" in url)
            return json.dumps(payload(key)).encode()

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            self.assertEqual(cameras.refresh(output, NOW, reader), 2)
            originals = {p.name: p.read_bytes() for p in output.glob("*.json")}
            for key in cameras.CAMERAS:
                manifest = json.loads(originals[key + ".json"])
                with Image.open(output / manifest["image"]) as image:
                    self.assertEqual(image.size, (manifest["width"], manifest["height"]))
                    self.assertEqual(image.width, 600 if key == "gibraltar" else 1200)
            self.assertEqual(list(output.glob("*.tmp")), [])
            requests.clear()
            self.assertEqual(cameras.refresh(output, NOW, reader), 2)
            self.assertEqual(len(requests), 2, "unchanged panoramas must not redownload")
            self.assertEqual(cameras.refresh(output, NOW, lambda *args: b"{}"), 0)
            for name, body in originals.items():
                self.assertEqual((output / name).read_bytes(), body)
            # A broken camera must not prevent the other one from refreshing.
            def partial(url, deadline, limit):
                return b"{}" if "camId=1985&" in url else reader(url, deadline, limit)
            self.assertEqual(cameras.refresh(output, NOW, partial), 1)


if __name__ == "__main__":
    unittest.main()
