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


def tight_payload():
    return {"code": 1, "data": {
        "cams": {"data": [{"id": "1986", "cn": "Gibraltar_2", "pv": 0, "lid": 388,
                            "img": f"Gibraltar_2_{STAMP}_1234.jpg", "p": "176.49", "fov": "65.33"}]},
        "locs": {"data": [{"id": 388, "lp": 0}]},
    }}


class CameraTests(unittest.TestCase):
    def test_metadata_identity_privacy_timestamp_and_filename_validation(self):
        p = payload("gibraltar")
        result = cameras.parse_panorama(json.dumps(p), "gibraltar", NOW)
        self.assertEqual(result["stamp"], STAMP)
        self.assertIn("/1986/2026/09/26/Gibraltar_2_", result["source_image"])
        for field, value in [("cam_id", "1985"), ("cam_name", "Other"),
                             ("cmlg_img_name", "../../bad.jpg"), ("cmlg_cam_fov", "NaN"),
                             ("cmlg_cam_azimuth", "inf"), ("cmlg_date", "2026-09-25")]:
            invalid = payload("gibraltar")
            invalid["data"]["1986"]["cur"][field] = value
            with self.assertRaises(ValueError):
                cameras.parse_panorama(json.dumps(invalid), "gibraltar", NOW)
        p["data"]["1986"]["cur"]["full"]["private"] = "1"
        with self.assertRaises(ValueError):
            cameras.parse_panorama(json.dumps(p), "gibraltar", NOW)
        for now in (NOW - timedelta(hours=1), NOW + timedelta(days=2)):
            with self.assertRaises(ValueError):
                cameras.parse_panorama(json.dumps(payload("gibraltar")), "gibraltar", now)

    def test_tight_view_is_gibraltar_two_with_real_bearing_and_no_panorama_suffix(self):
        result = cameras.parse_tight(json.dumps(tight_payload()), NOW)
        self.assertIn('/1986/2026/09/26/Gibraltar_2_', result['source_image'])
        self.assertNotIn('_p.jpg', result['source_image'])
        self.assertEqual(result['azimuth'], 176.49)
        self.assertEqual(result['view_kind'], 'tight')
        for field, value in [('id', '1985'), ('cn', 'Gibraltar_1'), ('pv', 1),
                             ('img', f'Gibraltar_2_{STAMP}_1234_p.jpg'), ('p', 'NaN'), ('fov', 'inf')]:
            invalid = tight_payload()
            invalid['data']['cams']['data'][0][field] = value
            with self.assertRaises(ValueError):
                cameras.parse_tight(json.dumps(invalid), NOW)
        invalid = tight_payload()
        invalid['data']['locs']['data'][0]['lp'] = 1
        with self.assertRaises(ValueError):
            cameras.parse_tight(json.dumps(invalid), NOW)
        for now in (NOW - timedelta(hours=1), NOW + timedelta(days=2)):
            with self.assertRaises(ValueError):
                cameras.parse_tight(json.dumps(tight_payload()), now)

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
        tight = io.BytesIO()
        Image.new("RGB", (1920, 1080), "gray").save(tight, "JPEG")
        requests = []

        def reader(url, deadline, limit):
            requests.append(url)
            if url == cameras.TIGHT_API:
                return json.dumps(tight_payload()).encode()
            if url.endswith(".jpg"):
                return tight.getvalue() if '/1986/' in url else raw.getvalue()
            key = next(k for k, v in cameras.CAMERAS.items() if f"camId={v[0]}&" in url)
            return json.dumps(payload(key)).encode()

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            # A slightly newer cached image from camera 1 must not block camera 2.
            (output / 'gibraltar.json').write_text(json.dumps({
                'camera_id': '1985', 'stamp': str(int(STAMP) + 60), 'image': 'gibraltar-old-v1.jpg'}))
            self.assertEqual(cameras.refresh(output, NOW, reader), 2)
            originals = {p.name: p.read_bytes() for p in output.glob("*.json")}
            for key in cameras.CAMERAS:
                manifest = json.loads(originals[key + ".json"])
                with Image.open(output / manifest["image"]) as image:
                    self.assertEqual(image.size, (manifest["width"], manifest["height"]))
                    self.assertEqual(image.width, 600 if key == "gibraltar" else 1200)
                    if key == "gibraltar":
                        self.assertEqual(manifest['camera_id'], '1986')
                        self.assertEqual(manifest['center_deg'], 176.49)
                        self.assertEqual(image.size, (600, 366))
            self.assertEqual(list(output.glob("*.tmp")), [])
            requests.clear()
            self.assertEqual(cameras.refresh(output, NOW, reader), 2)
            self.assertEqual(len(requests), 2, "unchanged panoramas must not redownload")
            self.assertEqual(cameras.refresh(output, NOW, lambda *args: b"{}"), 0)
            for name, body in originals.items():
                self.assertEqual((output / name).read_bytes(), body)
            # A broken camera must not prevent the other one from refreshing.
            def partial(url, deadline, limit):
                return b"{}" if url == cameras.TIGHT_API else reader(url, deadline, limit)
            self.assertEqual(cameras.refresh(output, NOW, partial), 1)


if __name__ == "__main__":
    unittest.main()
