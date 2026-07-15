#!/usr/bin/env python3
"""Fetch public-domain aerial imagery for a case address — zero keys, zero attribution.

Chain: US Census geocoder (address -> lat/lon, public domain, keyless) ->
USGS NAIP via the USGSNAIPPlus ArcGIS ImageServer exportImage (public domain, ~0.6-1m GSD).
Emits a zoom ladder (wide -> mid -> tight) so SatelliteLocator can animate a real zoom-in,
plus meta JSON with the pixel coordinate of the address at each level.

Usage:
  fetch_aerial.py --address "4400 Fanuel St, San Diego, CA" --out <dir> [--label fanuel]
  fetch_aerial.py --latlon 32.79,-117.25 --out <dir>
"""
import argparse
import json
import math
import os
import urllib.parse
import urllib.request

NAIP = ("https://imagery.nationalmap.gov/arcgis/rest/services/"
        "USGSNAIPPlus/ImageServer/exportImage")
CENSUS = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

# zoom ladder: half-width of the view in meters
LADDER = [("wide", 1400.0), ("mid", 420.0), ("tight", 130.0)]
W, H = 1920, 1080


def geocode(address: str):
    q = urllib.parse.urlencode({"address": address, "benchmark": "Public_AR_Current",
                                "format": "json"})
    with urllib.request.urlopen(f"{CENSUS}?{q}", timeout=30) as r:
        d = json.load(r)
    m = d["result"]["addressMatches"]
    if not m:
        raise SystemExit(f"census geocoder: no match for {address!r}")
    c = m[0]["coordinates"]
    return float(c["y"]), float(c["x"]), m[0].get("matchedAddress", address)


def webmerc(lat, lon):
    R = 6378137.0
    x = math.radians(lon) * R
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * R
    return x, y


def fetch(lat, lon, half_w_m, out_png):
    x, y = webmerc(lat, lon)
    half_h_m = half_w_m * H / W
    bbox = f"{x-half_w_m},{y-half_h_m},{x+half_w_m},{y+half_h_m}"
    q = urllib.parse.urlencode({
        "bbox": bbox, "bboxSR": 3857, "imageSR": 3857, "size": f"{W},{H}",
        "format": "png", "f": "image"})
    url = f"{NAIP}?{q}"
    with urllib.request.urlopen(url, timeout=120) as r, open(out_png, "wb") as f:
        f.write(r.read())
    return os.path.getsize(out_png)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--address")
    ap.add_argument("--latlon")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="scene")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.latlon:
        lat, lon = (float(v) for v in a.latlon.split(","))
        matched = a.latlon
    else:
        lat, lon, matched = geocode(a.address)
    meta = {"label": a.label, "lat": lat, "lon": lon, "matched_address": matched,
            "source": "USGS NAIPPlus (public domain) via imagery.nationalmap.gov; "
                      "geocode: US Census (public domain)",
            "levels": []}
    for name, half in LADDER:
        png = f"{a.out}/{a.label}_{name}.png"
        size = fetch(lat, lon, half, png)
        # address is at the exact center at every level by construction
        meta["levels"].append({"name": name, "half_width_m": half, "file": os.path.basename(png),
                               "px": [W // 2, H // 2], "bytes": size})
        print(f"  {name:5} half={half:6.0f}m  {size//1024}KB -> {png}")
    json.dump(meta, open(f"{a.out}/{a.label}_aerial.json", "w"), indent=1)
    print(f"meta -> {a.out}/{a.label}_aerial.json")


if __name__ == "__main__":
    main()
