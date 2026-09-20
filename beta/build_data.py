#!/usr/bin/env python3
"""Refresh only beta/data.json. The primary site is a read-only input."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import re
import urllib.request

UTC = timezone.utc
METAR_URL = "https://aviationweather.gov/api/data/metar?ids=KSBA&format=json"
SATELLITE_URL = "https://cdn.star.nesdis.noaa.gov/WFO/lox/GEOCOLOR/"
CAMERA_URL = "https://ops.alertcalifornia.org/api/getCameraDataByLoc"
CAMERA_PAGE = "https://ops.alertcalifornia.org/cam-console/1986"
IMAGE_ORIGIN = "https://img.cdn.prod.alertwest.com"
MAX_BYTES = 16 * 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("unexpected feed redirect")


def fetch(url):
    if not url.startswith("https://"):
        raise ValueError("HTTPS required")
    request = urllib.request.Request(url, headers={"User-Agent": "SB-Live-Lapse-beta (https://sb-live-lapse.com)"})
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=20) as response:
        body = response.read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        raise ValueError("oversized feed")
    return body.decode("utf-8")


def number(value, low, high):
    if isinstance(value, bool) or value is None:
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) and low <= value <= high else None
    except (ValueError, TypeError):
        return None


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    except (ValueError, TypeError):
        return None


def parse_metar(raw, now):
    reports = json.loads(raw)
    if not isinstance(reports, list):
        raise ValueError("expected METAR list")
    candidates = [r for r in reports if isinstance(r, dict) and r.get("icaoId") == "KSBA"
                  and number(r.get("obsTime"), 0, now.timestamp()) is not None]
    if not candidates:
        raise ValueError("no matching non-future KSBA report")
    report = max(candidates, key=lambda r: float(r["obsTime"]))
    elevation = number(report.get("elev"), -500, 9000)
    if elevation is None:
        raise ValueError("airport elevation missing")
    layers = []
    for layer in report.get("clouds") or []:
        if not isinstance(layer, dict):
            continue
        cover = layer.get("cover")
        base = number(layer.get("base"), 0, 60000)
        if cover in ("FEW", "SCT", "BKN", "OVC") and base is not None:
            layers.append({"cover": cover, "base_agl_ft": base, "base_msl_m": base * 0.3048 + elevation})
    layers.sort(key=lambda r: r["base_msl_m"])
    cover = report.get("cover")
    raw_ob = str(report.get("rawOb") or "")[:1200]
    obscured = cover == "VV" or bool(re.search(r"\bVV(?:\d{3}|///)\b", raw_ob))
    clear = cover in ("CLR", "SKC", "NSC", "NCD") or any(
        isinstance(c, dict) and c.get("cover") in ("CLR", "SKC", "NSC", "NCD") for c in report.get("clouds") or [])
    sky = "layers" if layers else "obscured" if obscured else "clear" if clear else "unknown"
    return {"station_id": "KSBA", "observed_at": iso(float(report["obsTime"])),
            "layers": layers, "sky": sky, "raw": raw_ob, "elevation_m": elevation,
            "url": METAR_URL, "service": "NOAA Aviation Weather Center"}


def parse_satellite(raw, now):
    matches = re.findall(r'href="(\d{11}_GOES18-ABI-lox-GEOCOLOR-600x600\.jpg)"', raw)
    candidates = []
    for filename in set(matches):
        try:
            observed = datetime.strptime(filename[:11], "%Y%j%H%M").replace(tzinfo=UTC)
        except ValueError:
            continue
        if observed <= now:
            candidates.append((observed, filename))
    if not candidates:
        raise ValueError("no dated GOES-West image")
    observed, filename = max(candidates)
    return {"observed_at": iso(observed.timestamp()), "image_url": SATELLITE_URL + filename,
            "url": "https://www.star.nesdis.noaa.gov/GOES/wfo.php?wfo=lox",
            "service": "GOES-West GeoColor · Los Angeles / Oxnard", "credit": "CIRA/NOAA"}


def parse_camera(raw, now):
    payload = json.loads(raw)
    if payload.get("code") != 1:
        raise ValueError("camera feed unavailable")
    data = payload["data"]
    camera = next((c for c in data["cams"]["data"] if str(c.get("id")) == "1986"), None)
    if not camera or camera.get("cn") != "Gibraltar_2" or camera.get("pv") not in (0, False):
        raise ValueError("public Gibraltar 2 camera missing")
    location = next((l for l in data["locs"]["data"] if l.get("id") == camera.get("lid")), None)
    if not location or location.get("lp") not in (0, False):
        raise ValueError("public camera location missing")
    filename = str(camera.get("img") or "")
    match = re.fullmatch(r"Gibraltar_2_(\d{10})_\d+\.jpg", filename)
    if not match or int(match[1]) > now.timestamp():
        raise ValueError("invalid or future camera image")
    observed = datetime.fromtimestamp(int(match[1]), UTC)
    return {"name": "Gibraltar 2", "camera_id": "1986", "observed_at": iso(observed.timestamp()),
            "image_url": f"{IMAGE_ORIGIN}/data/img/1986/{observed:%Y/%m/%d}/{filename}",
            "latitude": number(location.get("lat"), -90, 90), "longitude": number(location.get("lon"), -180, 180),
            "heading_deg": number(camera.get("p"), 0, 360), "offline": bool(camera.get("off")),
            "url": CAMERA_PAGE, "service": "ALERTCalifornia", "credit": "ALERTCalifornia | UC San Diego"}


def load_profile(primary_dir, now):
    if primary_dir:
        # Resolve the primary's atomic release symlink once. Never write here.
        root = Path(primary_dir).resolve(strict=True)
        raw = (root / "station_history.json").read_text()
    else:
        raw = fetch("https://sb-live-lapse.com/station_history.json")
    snapshots = json.loads(raw).get("snapshots", [])
    valid = [(parse_time(s.get("run_at")), s) for s in snapshots if isinstance(s, dict)]
    valid = [(t, s) for t, s in valid if t is not None and t <= now and isinstance(s.get("stations"), dict)]
    if not valid:
        raise ValueError("primary has no usable snapshot")
    _, latest = max(valid, key=lambda x: x[0])
    return {"observed_at": latest["run_at"], "stations": latest["stations"], "rass": latest.get("rass", {})}


def build(primary_dir=None, previous=None, now=None):
    now = now or datetime.now(UTC)
    previous = previous or {}
    jobs = {
        "profile": lambda: load_profile(primary_dir, now),
        "airport": lambda: parse_metar(fetch(METAR_URL), datetime.now(UTC)),
        "satellite": lambda: parse_satellite(fetch(SATELLITE_URL), datetime.now(UTC)),
        "camera": lambda: parse_camera(fetch(CAMERA_URL), datetime.now(UTC)),
    }
    output = {"generated_at": iso(now.timestamp()), "version": 1}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {key: pool.submit(fn) for key, fn in jobs.items()}
        for key, future in futures.items():
            try:
                output[key] = {**future.result(), "fetch_ok": True}
            except Exception as exc:
                logging.warning("beta %s: %s", key, exc)
                # Retain actual observation time; refreshing never makes old data new.
                cached = previous.get(key)
                output[key] = {**(cached if isinstance(cached, dict) else {}), "fetch_ok": False}
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-dir")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    previous = {}
    if args.output.exists():
        try:
            previous = json.loads(args.output.read_text())
        except (ValueError, OSError):
            pass
    data = build(args.primary_dir, previous)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
    temporary.replace(args.output)
    print(json.dumps({"generated_at": data["generated_at"], "sources": {k: data[k]["fetch_ok"] for k in ("profile", "airport", "satellite", "camera")}}))


if __name__ == "__main__":
    main()
