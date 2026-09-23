#!/usr/bin/env python3
"""Add T/Td labels to read-only primary SVGs, preserving their chart geometry."""
import argparse
import json
import math
from pathlib import Path
import re
import xml.etree.ElementTree as ET

SVG = "{http://www.w3.org/2000/svg}"
ET.register_namespace("", SVG[1:-1])
NAMES = {"KC6OYN": "La Cumbre", "SE068": "VOR", "SE234": "AntFarm",
         "MTIC1": "Montecito", "MPWC1": "SM Pass", "421SE": "Parma", "KSBA": "Airport"}
SNAPSHOT_PATH = re.compile(r"^snapshots/\d{8}T\d{4}Z_(metric|imperial)\.svg$")


def value(raw):
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    return raw if math.isfinite(raw) and -100 <= raw <= 60 else None


def temperature_pair(row, unit):
    temperature, dew = value(row.get("temp_c")), value(row.get("dew_c"))
    if temperature is not None and dew is not None and dew > temperature:
        dew = None
    def formatted(number):
        if number is None:
            return "—"
        if unit == "imperial":
            number = number * 9 / 5 + 32
        return f"{number:.1f}{'F' if unit == 'imperial' else 'C'}"
    return f"{formatted(temperature)}/{formatted(dew)}"


def add_dew_points(svg, stations, unit):
    root = ET.fromstring(svg)
    rows = {key: {**stations.get(key, {}), "name": stations.get(key, {}).get("name") or name}
            for key, name in NAMES.items()}
    by_name = {row["name"]: row for row in rows.values()}
    label_right = float(root.get("viewBox").split()[2]) - 12
    elements = list(root)
    for index, element in enumerate(elements):
        kind = element.get("class")
        if kind == "station-label":
            spans = element.findall(SVG + "tspan")
            row = by_name[spans[0].text]
            spans[1].text = " " + temperature_pair(row, unit)
            # Use the original font-size estimate, allowing text into the existing
            # right margin before flipping. This avoids moving labels onto RASS.
            label = "".join(element.itertext())
            if element.get("text-anchor") == "start" and float(element.get("x")) + 6 * len(label) + 2 > label_right:
                new_x = f"{float(element.get('x')) - 12:.2f}"
                element.set("x", new_x)
                element.set("text-anchor", "end")
                deviation = elements[index + 1]
                if str(deviation.get("class", "")).startswith("station-dev-"):
                    deviation.set("x", new_x)
                    deviation.set("text-anchor", "end")
        elif kind == "legend-row":
            # Some original rows have plain text; others use colored lapse-rate spans.
            prefix_node = element[0] if len(element) else element
            prefix = prefix_node.text or ""
            station_id = next((key for key in rows if
                               f" ({'KC60YN' if key == 'KC6OYN' else key}) " in prefix), None)
            if station_id is None:
                raise ValueError("Unrecognized primary station row")
            replaced, count = re.subn(r" - (?:-?\d+(?:\.\d+)?[CF]|temp missing),",
                                     " - " + temperature_pair(rows[station_id], unit) + ",", prefix, count=1)
            if count != 1:
                raise ValueError("Unrecognized primary temperature label")
            prefix_node.text = replaced
        elif kind == "legend-h" and element.text == "Stations":
            element.text = "Stations (temperature/dew point)"
    return ET.tostring(root, encoding="unicode")


def write_atomic(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text() == body:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(body)
    temporary.replace(path)


def build(primary_dir, output_dir):
    primary = Path(primary_dir).resolve(strict=True)
    output = Path(output_dir).resolve()
    if output == primary or primary in output.parents:
        raise ValueError("Beta output must be separate from the primary release")
    state_text = (primary / "station_state.json").read_text()
    state = json.loads(state_text)
    history = json.loads((primary / "station_history.json").read_text())
    latest = {}
    for unit in ("metric", "imperial"):
        name = f"sba_wwtemp_chart_{unit}.svg"
        latest[name] = add_dew_points((primary / name).read_text(), state["stations"], unit)

    kept_snapshots, kept_paths = [], set()
    for snapshot in history["snapshots"]:
        charts = snapshot.get("charts") or {}
        paths = [charts.get(f"{unit}_svg") for unit in ("metric", "imperial")]
        if not all(isinstance(p, str) and SNAPSHOT_PATH.fullmatch(p) and (primary / p).is_file() for p in paths):
            continue
        for unit, path in zip(("metric", "imperial"), paths):
            svg = add_dew_points((primary / path).read_text(), snapshot["stations"], unit)
            write_atomic(output / path, svg)
            kept_paths.add(path)
        kept_snapshots.append(snapshot)

    for name, svg in latest.items():
        write_atomic(output / name, svg)
    write_atomic(output / "sba_wwtemp_chart.svg", latest["sba_wwtemp_chart_metric.svg"])
    write_atomic(output / "station_state.json", state_text)
    history["snapshots"] = kept_snapshots
    history["snapshot_count"] = len(kept_snapshots)
    write_atomic(output / "station_history.json", json.dumps(history, separators=(",", ":")) + "\n")
    for path in (output / "snapshots").glob("*.svg"):
        if SNAPSHOT_PATH.fullmatch(path.relative_to(output).as_posix()) and path.relative_to(output).as_posix() not in kept_paths:
            path.unlink()
    return {"generated_at": state["generated_at"], "snapshots": len(kept_snapshots)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.primary_dir, args.output_dir)))
