#!/usr/bin/env python3
"""Render a small, north-up GOES-West crop; all upstream traffic stays on the server."""
import argparse
from datetime import datetime, timedelta, timezone
import io
import json
import math
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont

BASE = "https://slider.cira.colostate.edu/data"
PRODUCTS = {"visible": ("band_02", 5), "night": ("eumetsat_nighttime_microphysics", 3)}
# Painted Cave to Rincon, plus about 4–6 km around the requested box.
WEST, SOUTH, EAST, NORTH = -119.85, 34.32, -119.42, 34.55
WIDTH, MAP_HEIGHT, TOP, HEIGHT = 600, 390, 28, 418
TILE_SIZE = 678
PACIFIC = ZoneInfo("America/Los_Angeles")
COASTLINE = Path(__file__).with_name("data") / "coastline.json"
RENDER_VERSION = 2


def parse_stamp(stamp):
    if not re.fullmatch(r"\d{14}", str(stamp)):
        raise ValueError("Invalid satellite timestamp")
    return datetime.strptime(str(stamp), "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def solar_elevation(moment, latitude=34.435, longitude=-119.635):
    """NOAA fractional-year approximation, sufficient for the day/night switch."""
    moment = moment.astimezone(timezone.utc)
    hour = moment.hour + moment.minute / 60 + moment.second / 3600
    gamma = 2 * math.pi / 365 * (moment.timetuple().tm_yday - 1 + (hour - 12) / 24)
    eqtime = 229.18 * (0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
                      - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma))
    decl = (0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)
            - 0.006758 * math.cos(2 * gamma) + 0.000907 * math.sin(2 * gamma)
            - 0.002697 * math.cos(3 * gamma) + 0.00148 * math.sin(3 * gamma))
    ha = math.radians(((hour * 60 + eqtime + 4 * longitude) % 1440) / 4 - 180)
    lat = math.radians(latitude)
    return math.degrees(math.asin(math.sin(lat) * math.sin(decl) + math.cos(lat) * math.cos(decl) * math.cos(ha)))


def mode_for(moment):
    return "visible" if solar_elevation(moment) > 0 else "night"


def satellite_xy(latitude, longitude, zoom):
    """Inverse of CIRA SLIDER's GOES-18 full-disk bigPixels2LatLon navigation.

    Constants are from its public define-products---rammb-slider.js catalog.
    This preserves that imagery's actual grid rather than guessing a JPEG crop.
    Coordinates describe the Earth's surface; elevated clouds have parallax.
    """
    a, b, distance = 6378.1, 6356.8, 42171.7
    lat, lon = math.radians(latitude), math.radians(longitude + 137.0)
    geocentric = math.atan((b / a) ** 2 * math.tan(lat))
    radius = b / math.sqrt(1 - (1 - (b / a) ** 2) * math.cos(geocentric) ** 2)
    sx = distance - radius * math.cos(geocentric) * math.cos(lon)
    sy = radius * math.cos(geocentric) * math.sin(lon)
    sz = radius * math.sin(geocentric)
    x = math.asin(sy / math.sqrt(sx * sx + sy * sy + sz * sz))
    y = math.atan2(sz, sx)
    factor = 2 ** zoom
    return (339 * factor + 0.5 + x / 0.151337 * 338 * factor,
            339 * factor + 0.5 - y / 0.150988 * 337 * factor)


def map_xy(latitude, longitude):
    return ((longitude - WEST) / (EAST - WEST) * WIDTH,
            TOP + (NORTH - latitude) / (NORTH - SOUTH) * MAP_HEIGHT)


def tile_url(stamp, mode, row, column):
    parse_stamp(stamp)
    product, zoom = PRODUCTS[mode]
    date = f"{stamp[:4]}/{stamp[4:6]}/{stamp[6:8]}"
    return f"{BASE}/imagery/{date}/goes-18---full_disk/{product}/{stamp}/{zoom:02}/{row:03}_{column:03}.png"


def fetch(url, deadline, limit=4_000_000):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Satellite refresh time budget exhausted")
    request = Request(url, headers={"User-Agent": "SB-Live-Lapse/1.0 (sb-live-lapse.com)"})
    with urlopen(request, timeout=min(12, remaining)) as response:
        data = response.read(limit + 1)
    if len(data) > limit:
        raise ValueError("Unexpectedly large satellite response")
    return data


def candidate_frames(catalogs, now):
    frames = []
    for mode, stamps in catalogs.items():
        for stamp in stamps:
            try:
                moment = parse_stamp(stamp)
            except (ValueError, TypeError):
                continue
            age = (now - moment).total_seconds()
            if 0 <= age <= 90 * 60 and mode_for(moment) == mode:
                frames.append((str(stamp), mode))
    return sorted(set(frames), reverse=True)


def frame_pixels(zoom):
    # Only ~80 by 40 distinct daytime samples, enlarged without invented detail.
    pixels = []
    tiles = set()
    for row in range(MAP_HEIGHT):
        lat = NORTH - (row + 0.5) / MAP_HEIGHT * (NORTH - SOUTH)
        for column in range(WIDTH):
            lon = WEST + (column + 0.5) / WIDTH * (EAST - WEST)
            x, y = satellite_xy(lat, lon, zoom)
            x, y = int(x), int(y)
            tile = (y // TILE_SIZE, x // TILE_SIZE)
            tiles.add(tile)
            pixels.append((tile, x % TILE_SIZE, y % TILE_SIZE))
    return pixels, tiles


def font(size):
    return ImageFont.load_default(size=size)


def render_frame(stamp, mode, tiles, pixels):
    output = Image.new("RGB", (WIDTH, HEIGHT), "white")
    tile_data = {key: value.convert("RGB").load() for key, value in tiles.items()}
    colors = [tile_data[tile][x, y] for tile, x, y in pixels]
    area = Image.new("RGB", (WIDTH, MAP_HEIGHT))
    area.putdata(colors)
    if mode == "visible":
        # A fixed gamma improves cloud contrast without per-frame flicker.
        gamma = [round(255 * (value / 255) ** 0.5) for value in range(256)]
        area = area.point(gamma * 3)
    output.paste(area, (0, TOP))
    draw = ImageDraw.Draw(output)
    for line in json.loads(COASTLINE.read_text()):
        points = [map_xy(lat, lon) for lon, lat in line]
        # Coastline is drawn on the map only, not over either caption strip.
        overlay = Image.new("RGBA", (WIDTH, MAP_HEIGHT))
        ImageDraw.Draw(overlay).line([(x, y - TOP) for x, y in points], fill=(255, 220, 70, 255), width=1)
        output.paste(overlay, (0, TOP), overlay)
    gold = "#ffdc46"
    sites = [("Painted Cave", 34.5048, -119.7879, 9, -24),
             ("Santa Barbara", 34.4208, -119.6982, -45, 10),
             ("Carpinteria", 34.3989, -119.5185, -108, -28),
             ("Rincon", 34.3737, -119.4783, -66, 10)]
    for name, lat, lon, dx, dy in sites:
        x, y = map_xy(lat, lon)
        draw.ellipse((x - 3, y - 3, x + 3, y + 3), fill=gold, outline="black", width=1)
        draw.text((x + dx, y + dy), name, font=font(18), fill=gold, stroke_width=1, stroke_fill="black")
    draw.text((WIDTH - 25, TOP + 10), "N", font=font(18), fill=gold, stroke_width=1, stroke_fill="black")
    scale = 5 / (111.32 * math.cos(math.radians(34.435))) / (EAST - WEST) * WIDTH
    y = TOP + MAP_HEIGHT - 15
    draw.line([(18, y), (18 + scale, y)], fill=gold, width=2)
    draw.text((18, y - 23), "5 km", font=font(16), fill=gold, stroke_width=1, stroke_fill="black")
    moment = parse_stamp(stamp)
    clock = moment.astimezone(PACIFIC).strftime("%b %d %H:%M %Z")
    draw.text((4, 4), clock, font=font(17), fill="#222222")
    return output


def atomic_write(path, body):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(body)
    temporary.replace(path)


def refresh(output, now=None, reader=fetch):
    now = now or datetime.now(timezone.utc)
    output.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 65
    catalogs = {}
    for mode, (product, _) in PRODUCTS.items():
        try:
            url = f"{BASE}/json/goes-18/full_disk/{product}/latest_times.json"
            catalogs[mode] = json.loads(reader(url, deadline, 100_000))["timestamps_int"]
        except Exception as error:
            print(f"Satellite {mode} catalog unavailable: {error}", flush=True)
    candidates = candidate_frames(catalogs, now)
    old = {}
    manifest_path = output / "latest.json"
    if manifest_path.exists():
        old = json.loads(manifest_path.read_text())
    geometry, frames, rendered = {}, [], []
    for stamp, mode in candidates:
        if frames and (parse_stamp(frames[0]["stamp"]) - parse_stamp(stamp)).total_seconds() > 3600:
            break
        if len(frames) >= 7 or time.monotonic() >= deadline:
            break
        name = f"{stamp}-{mode}-v{RENDER_VERSION}.jpg"
        path = output / name
        try:
            if path.exists():
                with Image.open(path) as saved:
                    frame = saved.convert("RGB")
            else:
                zoom = PRODUCTS[mode][1]
                if zoom not in geometry:
                    geometry[zoom] = frame_pixels(zoom)
                pixels, keys = geometry[zoom]
                tiles = {}
                for row, column in keys:
                    raw = reader(tile_url(stamp, mode, row, column), deadline)
                    with Image.open(io.BytesIO(raw)) as tile:
                        if tile.size != (TILE_SIZE, TILE_SIZE) or tile.format != "PNG":
                            raise ValueError("Unexpected satellite tile format or dimensions")
                        tiles[(row, column)] = tile.convert("RGB")
                frame = render_frame(stamp, mode, tiles, pixels)
                content = io.BytesIO()
                frame.save(content, "JPEG", quality=85, optimize=True, subsampling=0)
                atomic_write(path, content.getvalue())
            frames.append({"stamp": stamp, "mode": mode, "image": name})
            rendered.append(frame)
        except Exception as error:
            print(f"Satellite frame {stamp} unavailable: {error}", flush=True)
    if not frames:
        raise RuntimeError("No recent satellite frame available; retaining the last good image")
    latest = frames[0]
    # An upstream partial outage must never replace a newer published image.
    if old.get("stamp", "") > latest["stamp"]:
        raise RuntimeError("Upstream imagery is older than the published frame; retaining it")
    loop_name = None
    if len(frames) >= 2:
        loop_name = f"{latest['stamp']}-{frames[-1]['stamp']}-{len(frames)}-v{RENDER_VERSION}.gif"
        loop_path = output / loop_name
        if not loop_path.exists():
            content = io.BytesIO()
            animation = list(reversed(rendered))
            animation[0].save(content, "GIF", save_all=True, append_images=animation[1:],
                              duration=[700] * (len(animation) - 1) + [1800], loop=0, disposal=2)
            atomic_write(loop_path, content.getvalue())
    observed = parse_stamp(latest["stamp"])
    manifest = dict(latest, observed_at=observed.isoformat().replace("+00:00", "Z"),
                    image_bytes=(output / latest["image"]).stat().st_size,
                    loop=loop_name, loop_bytes=(output / loop_name).stat().st_size if loop_name else 0,
                    frames=list(reversed(frames)), twilight=abs(solar_elevation(observed)) < 5,
                    bounds=[WEST, SOUTH, EAST, NORTH], render_version=RENDER_VERSION)
    atomic_write(manifest_path, (json.dumps(manifest, separators=(",", ":")) + "\n").encode())
    # Leave 24 hours for cached manifests and in-flight readers. Only own artifacts.
    for path in output.iterdir():
        if re.fullmatch(r"\d{14}-(?:visible|night|\d{14}-\d+)-v\d+\.(?:jpg|gif)", path.name):
            if (now - parse_stamp(path.name[:14])).total_seconds() > 86400:
                path.unlink()
    print(f"Satellite {latest['stamp']} {latest['mode']}: {manifest['image_bytes']} bytes, "
          f"{len(frames)} loop frames / {manifest['loop_bytes']} bytes", flush=True)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    refresh(args.output_dir)
