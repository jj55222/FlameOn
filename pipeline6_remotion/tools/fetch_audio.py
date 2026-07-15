#!/usr/bin/env python3
"""Seed the FlameOn royalty-free audio library — attribution-free sources only.

MUSIC: Mixkit stock music (Mixkit License — free for commercial video, no attribution).
       (FreePD.com shut down 2026 — do not re-add.)
SFX:   Mixkit (same license).
Every file is logged to audio_library/LICENSES.md with source URL + license.
Genuine-recording bias for SFX (radio clicks, sirens, crowd, footsteps).

Usage: fetch_audio.py --out audio_library [--max-per 4]
"""
import argparse
import html
import os
import re
import urllib.request

UA = {"User-Agent": "Mozilla/5.0 (FlameOn library builder; contact: local use)"}

MIXKIT_MUSIC = ["cinematic", "ambient", "sad", "suspense"]        # music moods for the genre
MIXKIT_PAGES = ["police", "gun", "heartbeat", "whoosh", "typewriter", "crowd", "siren"]


def get(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def save(url, path):
    data = get(url)
    with open(path, "wb") as f:
        f.write(data)
    return len(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="audio_library")
    ap.add_argument("--max-per", type=int, default=4)
    a = ap.parse_args()
    mus, sfx = f"{a.out}/music", f"{a.out}/sfx"
    os.makedirs(mus, exist_ok=True)
    os.makedirs(sfx, exist_ok=True)
    log = [
        "# FlameOn audio library — licenses",
        "",
        "All entries are attribution-free for commercial video use.",
        "| file | source | license |",
        "|---|---|---|",
    ]

    for page in MIXKIT_MUSIC:
        try:
            body = get(f"https://mixkit.co/free-stock-music/{page}/").decode("utf-8", "ignore")
        except Exception as e:
            print(f"mixkit music {page}: {e}")
            continue
        urls = list(dict.fromkeys(re.findall(
            r'(https://assets\.mixkit\.co/(?:active_storage/)?music/[^"\']+?\.(?:mp3|wav))', body)))
        titles = re.findall(r'data-title="([^"]+)"', body)
        for j, u in enumerate(urls[: a.max_per]):
            t = re.sub(r"[^a-z0-9]+", "_", html.unescape(titles[j]).lower()) if j < len(titles) else f"track_{j}"
            dst = f"{mus}/mixkit_{page}_{t}{os.path.splitext(u)[1]}"
            if os.path.exists(dst):
                continue
            try:
                n = save(u, dst)
                log.append(f"| music/{os.path.basename(dst)} | {u} | Mixkit License (free commercial, no attribution) |")
                print(f"music  {page}/{t} {n//1024}KB")
            except Exception as e:
                print(f"  skip {page}/{t}: {e}")

    for page in MIXKIT_PAGES:
        try:
            body = get(f"https://mixkit.co/free-sound-effects/{page}/").decode("utf-8", "ignore")
        except Exception as e:
            print(f"mixkit {page}: {e}")
            continue
        urls = re.findall(r'(https://assets\.mixkit\.co/active_storage/sfx/\d+/\d+[^"\']*?\.(?:wav|mp3))', body)
        titles = re.findall(r'data-title="([^"]+)"', body)
        seen = []
        for u in urls:
            if u in seen:
                continue
            seen.append(u)
        for j, u in enumerate(seen[: a.max_per]):
            t = re.sub(r"[^a-z0-9]+", "_", html.unescape(titles[j]).lower()) if j < len(titles) else f"sfx_{j}"
            dst = f"{sfx}/mixkit_{page}_{t}{os.path.splitext(u)[1]}"
            if os.path.exists(dst):
                continue
            try:
                n = save(u, dst)
                log.append(f"| sfx/{os.path.basename(dst)} | {u} | Mixkit License (free commercial, no attribution) |")
                print(f"sfx    {page}/{t} {n//1024}KB")
            except Exception as e:
                print(f"  skip {page}: {e}")

    with open(f"{a.out}/LICENSES.md", "w") as f:
        f.write("\n".join(log) + "\n")
    nm = len(os.listdir(mus))
    ns = len(os.listdir(sfx))
    print(f"library: {nm} music · {ns} sfx · manifest {a.out}/LICENSES.md")


if __name__ == "__main__":
    main()
