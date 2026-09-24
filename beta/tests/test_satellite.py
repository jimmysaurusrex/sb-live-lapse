import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from PIL import Image

spec = importlib.util.spec_from_file_location("satellite", Path(__file__).parents[1] / "build_satellite.py")
satellite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(satellite)


class SatelliteTests(unittest.TestCase):
    def test_geographic_registration_and_tile_selection(self):
        # Reference values from CIRA's GOES-18 full-disk navigation; a wrong
        # projection, longitude, scale, row or column would shift the coastline.
        x, y = satellite.satellite_xy(34.4208, -119.6982, 5)
        self.assertAlmostEqual(x, 13846.8584, places=3)
        self.assertAlmostEqual(y, 3975.9382, places=3)
        self.assertEqual((int(y) // 678, int(x) // 678), (5, 20))
        x, y = satellite.satellite_xy(34.4208, -119.6982, 3)
        self.assertEqual((int(y) // 678, int(x) // 678), (1, 5))
        self.assertEqual(satellite.map_xy(satellite.NORTH, satellite.WEST), (0, satellite.TOP))
        self.assertEqual(satellite.map_xy(satellite.SOUTH, satellite.EAST),
                         (satellite.WIDTH, satellite.TOP + satellite.MAP_HEIGHT))
        self.assertTrue(satellite.tile_url('20260924003021', 'visible', 5, 20).endswith(
            '/2026/09/24/goes-18---full_disk/band_02/20260924003021/05/005_020.png'))

    def test_day_night_selection_uses_sun_position_not_fixed_clock_hours(self):
        self.assertEqual(satellite.mode_for(satellite.parse_stamp('20260923200021')), 'visible')
        self.assertEqual(satellite.mode_for(satellite.parse_stamp('20260923100021')), 'night')
        # 17:00 Pacific is daylight in June and darkness in December.
        self.assertEqual(satellite.mode_for(satellite.parse_stamp('20260624000021')), 'visible')
        self.assertEqual(satellite.mode_for(satellite.parse_stamp('20261224010021')), 'night')

    def test_catalog_rejects_wrong_product_future_invalid_and_old_frames(self):
        now = satellite.parse_stamp('20260924004500')
        stamps = ['20260924003021', '20260924002021', '20260924010021',
                  '20260923000021', '../../x', None]
        frames = satellite.candidate_frames({'visible': stamps, 'night': stamps}, now)
        self.assertEqual(frames, [('20260924003021', 'visible'), ('20260924002021', 'visible')])

    def test_real_crop_and_loop_publish_atomically_and_reuse_downloads(self):
        now = satellite.parse_stamp('20260924004500')
        stamps = ['20260924003021', '20260924002021', '20260924001021']
        requests = []

        def reader(url, deadline, limit=None):
            requests.append(url)
            if url.endswith('latest_times.json'):
                return json.dumps({'timestamps_int': stamps}).encode()
            shade = 60 + 20 * next(i for i, stamp in enumerate(stamps) if stamp in url)
            data = io.BytesIO()
            Image.new('RGB', (678, 678), (shade, shade, shade)).save(data, 'PNG')
            return data.getvalue()

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            manifest = satellite.refresh(output, now, reader)
            self.assertEqual(manifest['stamp'], stamps[0])
            self.assertEqual([f['stamp'] for f in manifest['frames']], list(reversed(stamps)))
            self.assertLess(manifest['image_bytes'], 80_000)
            self.assertEqual(len([u for u in requests if u.endswith('.png')]), 3)
            with Image.open(output / manifest['image']) as image:
                self.assertEqual(image.size, (600, 466))
            with Image.open(output / manifest['loop']) as loop:
                self.assertEqual(loop.n_frames, 3)
            self.assertEqual(json.loads((output / 'latest.json').read_text()), manifest)
            self.assertEqual(list(output.glob('*.tmp')), [])
            requests.clear()
            satellite.refresh(output, now, reader)
            self.assertFalse(any(u.endswith('.png') for u in requests), 'unchanged frames must be reused')
            previous = (output / 'latest.json').read_bytes()
            with self.assertRaises(RuntimeError):
                satellite.refresh(output, now, lambda *args: b'{"timestamps_int": []}')
            self.assertEqual((output / 'latest.json').read_bytes(), previous)
            stamps.pop(0)
            with self.assertRaisesRegex(RuntimeError, 'older'):
                satellite.refresh(output, now, reader)
            self.assertEqual((output / 'latest.json').read_bytes(), previous)

    def test_incomplete_newest_tile_uses_previous_available_scan(self):
        now = satellite.parse_stamp('20260924004500')
        stamps = ['20260924003021', '20260924002021']
        def reader(url, deadline, limit=None):
            if url.endswith('latest_times.json'):
                return json.dumps({'timestamps_int': stamps}).encode()
            if stamps[0] in url:
                raise OSError('tile still being published')
            data = io.BytesIO()
            Image.new('RGB', (678, 678), 'gray').save(data, 'PNG')
            return data.getvalue()
        with tempfile.TemporaryDirectory() as temporary:
            manifest = satellite.refresh(Path(temporary), now, reader)
            self.assertEqual(manifest['stamp'], stamps[1])
            self.assertIsNone(manifest['loop'])


if __name__ == '__main__':
    unittest.main()
