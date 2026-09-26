#!/usr/bin/env python3
"""Cache compact public ALERTCalifornia views independently of chart generation."""
import argparse
from datetime import datetime, timezone
import io
import json
import math
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont

API = "https://api.cdn.prod.alertwest.com/api/panorama/list/byCamId"
IMAGES = "https://img.cdn.prod.alertwest.com/data/img"
PACIFIC = ZoneInfo("America/Los_Angeles")
VERSION = 2
CAMERAS = {
    "gibraltar": ("1985", "Gibraltar_1", 180, 65.33, 600),
    "tvhill": ("2748", "TV_Hill_2", 30, 120, 1200),
}


def fetch(url, deadline, limit):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Camera refresh budget exceeded")
    request = Request(url, headers={"User-Agent": "SB-Live-Lapse/1.0"})
    with urlopen(request, timeout=min(12, remaining)) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("Camera response too large")
    return body


def parse_panorama(raw, key, now):
    camera_id, name, _, _, _ = CAMERAS[key]
    payload = json.loads(raw)
    if payload.get("code") != 1:
        raise ValueError("Camera metadata unavailable")
    data = payload["data"][camera_id]["cur"]
    if (str(data["cam_id"]) != camera_id or data["cam_name"] != name or
            str(data["full"].get("private")) != "0"):
        raise ValueError("Public camera identity mismatch")
    filename = data["cmlg_img_name"]
    match = re.fullmatch(re.escape(name) + r"_(\d{10})_\d+_p\.jpg", filename)
    if not match or str(data["cmlg_timestamp"]) != match[1]:
        raise ValueError("Invalid camera filename or timestamp")
    observed = datetime.fromtimestamp(int(match[1]), timezone.utc)
    if observed > now or (now - observed).total_seconds() > 24 * 3600:
        raise ValueError("Camera panorama is future-dated or over a day old")
    if data["cmlg_date"] != observed.strftime("%Y-%m-%d"):
        raise ValueError("Camera date mismatch")
    azimuth, fov = float(data["cmlg_cam_azimuth"]), float(data["cmlg_cam_fov"])
    if not math.isfinite(azimuth) or not 0 <= azimuth <= 360 or not 360 <= fov <= 420:
        raise ValueError("Invalid panorama coverage")
    return {"observed_at": observed.isoformat().replace("+00:00", "Z"),
            "stamp": match[1], "azimuth": azimuth, "fov": fov,
            "source_image": f"{IMAGES}/{camera_id}/{observed:%Y/%m/%d}/{filename}"}


def direction_view(source, azimuth, fov, center, span, width):
    """Extract actual pixels, wrapping at north; source azimuth is its midpoint.

    ALERTCalifornia's ~392-degree panorama includes overlap at both edges.
    Keep one complete revolution, then orient/crop it to the requested bearing.
    No synthesis, horizon warping, or changes to the observed clouds.
    """
    if not (source.width >= 3600 and source.height >= 300 and
            8 <= source.width / source.height <= 15):
        raise ValueError("Unexpected panorama dimensions")
    period = round(source.width * 360 / fov)
    start = ((center - span / 2 - (azimuth - fov / 2)) % 360) / 360 * period
    extent = round(period * span / 360)
    left = round(start) % period
    result = Image.new("RGB", (extent, source.height))
    first = min(extent, period - left)
    result.paste(source.crop((left, 0, left + first, source.height)), (0, 0))
    if first < extent:
        result.paste(source.crop((0, 0, extent - first, source.height)), (first, 0))
    height = round(source.height * width / extent)
    return result.resize((width, height), Image.Resampling.LANCZOS)


def render(raw, metadata, key):
    _, _, center, span, width = CAMERAS[key]
    with Image.open(io.BytesIO(raw)) as source:
        if source.format != "JPEG" or source.width * source.height > 16_000_000:
            raise ValueError("Invalid camera image")
        view = direction_view(source, metadata["azimuth"], metadata["fov"], center, span, width)
    # Match the satellite timestamp strip and label the cropped panorama bearings.
    top, bottom = 28, 20 if key == "tvhill" else 0
    output = Image.new("RGB", (width, view.height + top + bottom), "white")
    output.paste(view, (0, top))
    draw = ImageDraw.Draw(output)
    observed = datetime.fromisoformat(metadata["observed_at"].replace("Z", "+00:00"))
    draw.text((4, 4), observed.astimezone(PACIFIC).strftime("%b %d %H:%M %Z"),
              font=ImageFont.load_default(size=17), fill="#222222")
    if key == "tvhill":
        for i in range(5):
            bearing = round(center - span / 2 + span * i / 4) % 360
            label = f"{bearing:03d}°" + ({0: " N", 90: " E"}.get(bearing, ""))
            text_font = ImageFont.load_default(size=14)
            text_width = draw.textlength(label, font=text_font)
            x = min(width - text_width - 4, max(4, i * width / 4 - text_width / 2))
            draw.text((x, top + view.height + 2), label, font=text_font, fill="#333333")
    encoded = io.BytesIO()
    output.save(encoded, format="JPEG", quality=83, optimize=True, progressive=True)
    return encoded.getvalue(), output.size


def atomic_write(path, body):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_bytes(body)
    temp.replace(path)


def refresh(output, now=None, reader=fetch):
    now = now or datetime.now(timezone.utc)
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 65
    successes = 0
    for key, (camera_id, _, center, span, _) in CAMERAS.items():
        path = output / f"{key}.json"
        try:
            metadata = parse_panorama(reader(f"{API}?camId={camera_id}&timestamp=", deadline, 100_000), key, now)
            filename = f"{key}-{metadata['stamp']}-v{VERSION}.jpg"
            old = json.loads(path.read_text()) if path.exists() else {}
            if old.get("stamp", "") > metadata["stamp"]:
                raise ValueError("Upstream camera regressed; retaining last good image")
            if old.get("image") == filename and (output / filename).exists():
                successes += 1
                continue
            raw = reader(metadata["source_image"], deadline, 8_000_000)
            body, size = render(raw, metadata, key)
            atomic_write(output / filename, body)
            manifest = dict(metadata, image=filename, image_bytes=len(body), camera_id=camera_id,
                            center_deg=center, span_deg=span, width=size[0], height=size[1],
                            render_version=VERSION, credit="ALERTCalifornia | UC San Diego")
            atomic_write(path, (json.dumps(manifest, indent=2) + "\n").encode())
            successes += 1
            print(f"{key}: {metadata['observed_at']} {len(body)} bytes", flush=True)
        except Exception as error:
            # Each feed is independent. Never replace a good cached view with an error.
            print(f"{key}: {error}", flush=True)
    # Keep a day of our own files so clients with an older manifest can finish.
    for path in output.glob("*.jpg"):
        match = re.fullmatch(r"(?:gibraltar|tvhill|ortega)-(\d{10})-v\d+\.jpg", path.name)
        if match and now.timestamp() - int(match[1]) > 24 * 3600:
            # Preserve the last good image even through a long upstream outage.
            if any(json.loads(p.read_text()).get("image") == path.name for p in output.glob("*.json")):
                continue
            path.unlink()
    return successes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not refresh(args.output_dir):
        raise SystemExit("No camera feeds refreshed; existing images retained")
