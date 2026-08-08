"""
Download all new case artifacts:
  - SF DPA MP3s via NextRequest /download 302 redirect to S3
  - San Diego Sheriff CIB videos via yt-dlp (audio-only)
"""
import json
import os
import re
import requests
from pathlib import Path

ROOT = Path(__file__).parent.parent
CACHE = ROOT / "pipeline3_audio" / "foia_cache"
CACHE.mkdir(parents=True, exist_ok=True)

# ── SF DPA downloads (direct) ────────────────────────────────────
SFDPA_DOWNLOADS = [
    ("0204-18_iad_reininger", "https://sfdpa.nextrequest.com/documents/11728593"),
    ("0045-19_dpa_hernandez", "https://sfdpa.nextrequest.com/documents/12536055"),
    ("0045-19_dpa_pai",       "https://sfdpa.nextrequest.com/documents/12536054"),
    ("44321-20_dpa_rabsatt",  "https://sfdpa.nextrequest.com/documents/11775381"),
    ("0210-14_dpa_dejesus",   "https://sfdpa.nextrequest.com/documents/13468341"),
]

# ── San Diego Sheriff CIB videos (yt-dlp, audio only) ────────────
SDS_VIDEOS = [
    ("sds_vista",      "https://www.youtube.com/watch?v=avmvvSIxytw"),
    ("sds_el_cajon",   "https://www.youtube.com/watch?v=WErLDy2sSzc"),
    ("sds_otay_mesa",  "https://www.youtube.com/watch?v=NWjzQ-UIGJY"),
]


def download_sfdpa():
    results = []
    for label, base_url in SFDPA_DOWNLOADS:
        dl_url = base_url + "/download"
        out_path = CACHE / f"sfdpa_{label}.mp3"
        if out_path.exists() and out_path.stat().st_size > 0:
            print(f"[SFDPA] SKIP (cached): {out_path.name} ({out_path.stat().st_size/1e6:.1f} MB)")
            results.append((label, str(out_path), "cached"))
            continue
        try:
            print(f"[SFDPA] {label} -> {dl_url}")
            r = requests.get(dl_url, allow_redirects=True, timeout=120,
                            headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.content) > 1000:
                out_path.write_bytes(r.content)
                print(f"  -> {out_path.name} ({len(r.content)/1e6:.1f} MB)")
                results.append((label, str(out_path), "downloaded"))
            else:
                print(f"  FAILED status={r.status_code} size={len(r.content)}")
                results.append((label, None, f"failed_{r.status_code}"))
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((label, None, f"error_{type(e).__name__}"))
    return results


def download_sds():
    import yt_dlp
    results = []
    for label, url in SDS_VIDEOS:
        out_tmpl = str(CACHE / f"sds_cib_{label}.%(ext)s")
        expected = CACHE / f"sds_cib_{label}.mp3"
        if expected.exists() and expected.stat().st_size > 0:
            print(f"[SDS] SKIP (cached): {expected.name}")
            results.append((label, str(expected), "cached"))
            continue
        opts = {
            "format": "bestaudio/best",
            "outtmpl": out_tmpl,
            "quiet": True,
            "noprogress": True,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "64",
            }],
        }
        try:
            print(f"[SDS] {label} -> {url}")
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            if expected.exists():
                print(f"  -> {expected.name} ({expected.stat().st_size/1e6:.1f} MB)")
                results.append((label, str(expected), "downloaded"))
            else:
                # yt-dlp may have used different ext
                for p in CACHE.glob(f"sds_cib_{label}.*"):
                    print(f"  -> {p.name}")
                    results.append((label, str(p), "downloaded"))
                    break
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((label, None, f"error_{type(e).__name__}"))
    return results


if __name__ == "__main__":
    print("=" * 70)
    print("Phase 1: SF DPA direct downloads")
    print("=" * 70)
    sfdpa = download_sfdpa()
    print()
    print("=" * 70)
    print("Phase 2: SDS CIB videos (yt-dlp)")
    print("=" * 70)
    sds = download_sds()
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"SF DPA: {sum(1 for _,p,_ in sfdpa if p)}/{len(sfdpa)}")
    print(f"SDS:    {sum(1 for _,p,_ in sds if p)}/{len(sds)}")
    manifest = {"sfdpa": sfdpa, "sds": sds}
    with open(ROOT / "discovered_cases" / "download_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
