"""P3.5 / GOAL_D — disk-safe media triage from inside a zip.

Large FOIA bundles arrive as one huge zip (the OIS case: 42 GB, 266 POVs) and
there's no room to extract it whole. This triages media WITHOUT full extraction:
for each selected member it streams the file out to a temp path, runs the cheap
audio salience + impulse (gunshot) scan, records the result, and DELETES the
temp file before moving on — so disk never holds more than one POV at a time.

The impulse/crest-factor scan in pov_triage is a gunshot detector by design (a
shot is a sharp broadband transient), so this is also how an OIS shooting moment
is found across many unlabeled cameras: rank POVs by impulse, the loud ones saw
the shots. A disk guard aborts before free space runs out.

Reuses pov_triage (scan_salience / ocr_clock). Global Python (ffmpeg + numpy).
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".m4v", ".avi", ".wmv"}


# ---------------------------------------------------------------------------
# Member selection (pure, testable)
# ---------------------------------------------------------------------------

def list_video_members(names_sizes: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """Video members as ``[(name, size)]`` from a (name,size) list."""
    return [(n, s) for n, s in names_sizes if Path(n).suffix.lower() in VIDEO_EXTS]


def densest_cluster(times: List[float], win_sec: float = 25.0) -> Dict:
    """The ``win_sec`` window containing the most impulses — a gunshot VOLLEY is a
    dense cluster, unlike scattered door-slams across a long recording. Returns
    ``{start, count}`` (pure). At 1-s scan resolution rapid fire shows up as a run
    of consecutive loud seconds, so a real volley clusters tightly."""
    if not times:
        return {"start": None, "count": 0}
    best = {"start": times[0], "count": 0}
    for t in times:
        c = sum(1 for u in times if t <= u < t + win_sec)
        if c > best["count"]:
            best = {"start": round(t, 1), "count": c}
    return best


def select_members(names_sizes: List[Tuple[str, int]], top_by_size: int = 0,
                   explicit: Optional[List[str]] = None,
                   min_size_mb: float = 0.0) -> List[str]:
    """Pick which members to triage. ``explicit`` wins; else the ``top_by_size``
    largest videos above ``min_size_mb`` (the biggest files are the long bodycam
    recordings most likely to hold the incident)."""
    vids = list_video_members(names_sizes)
    if explicit:
        want = set(explicit)
        return [n for n, _ in vids if n in want]
    vids = [(n, s) for n, s in vids if s >= min_size_mb * 1e6]
    vids.sort(key=lambda x: -x[1])
    return [n for n, _ in (vids[:top_by_size] if top_by_size else vids)]


# ---------------------------------------------------------------------------
# Disk-safe triage loop
# ---------------------------------------------------------------------------

def free_gb(path: str = ".") -> float:
    return shutil.disk_usage(os.path.abspath(path)).free / 1e9


def _incident_epoch(incident_iso: str) -> Optional[float]:
    """'2017-08-30 12:00' -> epoch (UTC convention, matching ocr_clock)."""
    import datetime as dt
    import re as _re
    m = _re.search(r"(\d{4})\D(\d{1,2})\D(\d{1,2})\D+(\d{1,2})\D(\d{2})", incident_iso or "")
    if not m:
        return None
    y, mo, d, hh, mm = (int(x) for x in m.groups())
    return dt.datetime(y, mo, d, hh, mm, tzinfo=dt.timezone.utc).timestamp()


def clock_crossref(records: List[Dict], incident_iso: str,
                   slack_sec: float = 1800.0) -> List[Dict]:
    """Recordings whose wall-clock span covers the incident time — i.e. the
    cameras ROLLING when it happened (the officer bodycams at the scene). Pure:
    each record needs ``start_epoch`` + ``duration_sec``. ``slack_sec`` pads the
    window for an approximate incident time. Sorted by closeness to the incident."""
    inc = _incident_epoch(incident_iso)
    if inc is None:
        return []
    out = []
    for r in records:
        se = r.get("start_epoch")
        if se is None:
            continue
        end = se + float(r.get("duration_sec", 0))
        if se - slack_sec <= inc <= end + slack_sec:
            mid = se + float(r.get("duration_sec", 0)) / 2
            out.append({**r, "_offset_from_incident_sec": round(inc - se, 1),
                        "_dist": abs(inc - mid)})
    out.sort(key=lambda r: r["_dist"])
    return out


def _probe_duration(path: str) -> float:
    import subprocess
    import pov_triage as pt
    out = subprocess.run([pt.FFPROBE, "-v", "error", "-show_entries", "format=duration",
                          "-of", "default=nw=1:nk=1", path], capture_output=True, text=True)
    try:
        return float(out.stdout.strip())
    except ValueError:
        return 0.0


def triage_zip(zip_path: str, members: List[str], do_ocr: bool = False,
               clock_only: bool = False,
               min_free_gb: float = 3.0, tmp_dir: str = ".tmp/_zip_triage") -> List[Dict]:
    import pov_triage as pt
    pt.FFMPEG, pt.FFPROBE = pt._ff()
    z = zipfile.ZipFile(zip_path)
    sizes = {i.filename: i.file_size for i in z.infolist()}
    tmp = Path(tmp_dir)
    tmp.mkdir(parents=True, exist_ok=True)
    results: List[Dict] = []
    for k, m in enumerate(members):
        size_mb = sizes.get(m, 0) / 1e6
        if free_gb(tmp_dir) < max(min_free_gb, size_mb / 1000 + 1):
            sys.stderr.write(f"[zip-triage] ABORT: low disk before {m}\n")
            break
        local = tmp / Path(m).name
        sys.stderr.write(f"  [{k+1}/{len(members)}] {m} ({size_mb:.0f} MB) ...\n")
        sys.stderr.flush()
        try:
            with z.open(m) as s, open(local, "wb") as d:
                shutil.copyfileobj(s, d, length=1 << 20)
            if clock_only:
                # fast path: AXON clock only (no audio decode) — identifies which
                # files are clocked bodycams and when they were rolling.
                dur = _probe_duration(str(local))
                ep, wc = pt.ocr_clock(str(local), dur)
                results.append({"member": m, "size_mb": round(size_mb, 1),
                                "duration_sec": round(dur, 1),
                                "start_epoch": ep, "wallclock": wc,
                                "is_clocked": ep is not None})
                continue
            sc = pt.scan_salience(str(local))
            imps = [round(x, 1) for x in sc.impulses]
            rec: Dict = {
                "member": m, "size_mb": round(size_mb, 1),
                "duration_sec": round(sc.duration_sec, 1),
                "activity_score": sc.activity_score,
                "impulse_score": sc.impulse_score,          # loudest transient strength
                "n_impulses": len(imps),
                "shot_cluster": densest_cluster(imps),      # the volley candidate
                "impulses_sec": imps[:200],
                "audio_start_sec": sc.audio_start_sec,
                "hot_windows": sc.hot_windows[:8],
                "wallclock": None,
            }
            if do_ocr:
                _ep, wc = pt.ocr_clock(str(local), sc.duration_sec)
                rec["wallclock"] = wc
            results.append(rec)
        except Exception as e:  # noqa: BLE001 — one bad file shouldn't kill the run
            sys.stderr.write(f"     skip ({type(e).__name__}: {str(e)[:80]})\n")
        finally:
            if local.exists():
                local.unlink()      # delete immediately — never hold two POVs
    # rank by VOLLEY density then activity (the shooting cameras float up)
    if not clock_only:
        results.sort(key=lambda r: (-r["shot_cluster"]["count"], -r["activity_score"]))
    return results


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D — disk-safe media triage from a zip")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--top-by-size", type=int, default=10, help="triage the N largest videos")
    ap.add_argument("--members", nargs="*", default=None, help="explicit member names")
    ap.add_argument("--min-size-mb", type=float, default=0.0)
    ap.add_argument("--ocr", action="store_true", help="also OCR the AXON clock (slow)")
    ap.add_argument("--clock-only", action="store_true",
                    help="OCR the AXON clock only (no audio scan) — find clocked bodycams + when")
    ap.add_argument("--incident", default=None, help="incident time 'YYYY-MM-DD HH:MM' to cross-reference")
    ap.add_argument("--min-free-gb", type=float, default=3.0)
    ap.add_argument("--out", type=Path, default=Path(".tmp/zip_triage.json"))
    args = ap.parse_args(argv)

    z = zipfile.ZipFile(args.zip)
    names_sizes = [(i.filename, i.file_size) for i in z.infolist() if not i.is_dir()]
    members = select_members(names_sizes, top_by_size=args.top_by_size,
                             explicit=args.members, min_size_mb=args.min_size_mb)
    print(f"[zip-triage] {len(members)} member(s) selected; free disk {free_gb():.1f} GB"
          + ("  [CLOCK-ONLY]" if args.clock_only else ""))
    results = triage_zip(args.zip, members, do_ocr=args.ocr, clock_only=args.clock_only,
                         min_free_gb=args.min_free_gb)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(results, indent=2), encoding="utf-8")

    if args.clock_only:
        clocked = [r for r in results if r.get("is_clocked")]
        print(f"\n{len(clocked)}/{len(results)} clocked (AXON bodycams):")
        for r in clocked:
            print(f"  {Path(r['member']).name[:14]:14s} {r['wallclock']}  ({r['duration_sec']/60:.0f} min)")
        if args.incident:
            hits = clock_crossref(results, args.incident)
            print(f"\n>>> {len(hits)} recording(s) ROLLING at the incident ({args.incident}):")
            for r in hits[:20]:
                print(f"  {Path(r['member']).name[:14]:14s} starts {r['wallclock']} "
                      f"-> incident at +{r['_offset_from_incident_sec']:.0f}s")
    else:
        print(f"\n{'member':14s}{'dur':>7s}{'activity':>9s}{'#imp':>6s}  volley_cluster")
        for r in results:
            c = r["shot_cluster"]
            vol = (f"{c['count']} in 25s @ {int(c['start'])}s ({int(c['start'])//60}:{int(c['start']) % 60:02d})"
                   if c["start"] is not None else "-")
            print(f"{Path(r['member']).name[:14]:14s}{r['duration_sec']:6.0f}s"
                  f"{r['activity_score']:9.0f}{r['n_impulses']:6d}  {vol}")
    print(f"[zip-triage] -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
