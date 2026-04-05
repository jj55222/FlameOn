"""
Parse discovered cases from:
  1. CIB YouTube playlists (free)
  2. SF DPA /documents index (Firecrawl)
Rank by narrative signal keywords, output top 10.
"""
import json
import re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent

# ── Signal keywords: positive = good for narrative, negative = reject ──
POSITIVE = {
    # High-signal moments
    "ois": 6, "officer involved": 6, "officer-involved": 6,
    "shooting": 5, "fatal": 5, "homicide": 5,
    "deputy involved": 5, "deadly force": 5, "use of force": 4,
    "in-custody death": 6, "in custody death": 6,
    "stabbing": 4, "knife": 3, "armed": 2, "gun": 2, "firearm": 3,
    "interview": 4, "interrogation": 5, "confession": 6, "admission": 5,
    "bodycam": 3, "body worn": 3, "bwc": 3, "body-worn": 3,
    "pursuit": 3, "standoff": 4, "barricade": 3,
    "taser": 2, "canine": 2, "k-9": 2,
    "hostage": 5, "kidnapping": 4,
}
NEGATIVE = {
    "protest": -8, "occupy": -8, "political": -4, "rally": -5,
    "demonstration": -5, "press conference": -4,
    "community briefing only": -2,
    "redacted": -1,  # Heavy redaction hurts
}


def score_text(text: str) -> tuple:
    """Return (score, matched_keywords)."""
    t = text.lower()
    score = 0
    hits = []
    for kw, pts in POSITIVE.items():
        if kw in t:
            score += pts
            hits.append(f"+{pts} {kw}")
    for kw, pts in NEGATIVE.items():
        if kw in t:
            score += pts
            hits.append(f"{pts} {kw}")
    return score, hits


def parse_sfdpa_docs() -> list:
    """Parse the SF DPA markdown, group by case folder, keep only folders with MP3 interviews."""
    data = json.load(open(ROOT / "sfdpa_docs.json", encoding="utf-8"))
    md = data["https://sfdpa.nextrequest.com/documents"]["markdown"]

    # Extract rows: | [Title](url) | [req](url) | date | dl | folder | ... |
    rows = re.findall(
        r"\|\s*\[([^\]]+)\]\(([^)]+)\)\s*\|\s*\[[^\]]*\][^|]*\|\s*([\d/]+)\s*\|\s*(\d+)\s*\|\s*([^\s|]*)\s*\|",
        md,
    )

    by_folder = defaultdict(list)
    for title, url, upload_date, downloads, folder in rows:
        by_folder[folder].append({
            "title": title,
            "url": url,
            "upload_date": upload_date,
            "downloads": int(downloads) if downloads.isdigit() else 0,
        })

    # Build candidates: one per folder with at least one MP3 (interview/interrogation)
    candidates = []
    for folder, docs in by_folder.items():
        if not folder or folder == "":
            continue
        has_mp3 = any(d["title"].lower().endswith(".mp3") for d in docs)
        has_video = any(
            any(ext in d["title"].lower() for ext in (".mp4", ".mov", "bwc", "bodycam", "body-worn", "video"))
            for d in docs
        )
        has_pdf = any(d["title"].lower().endswith(".pdf") for d in docs)
        # Score the combined titles
        combined = " | ".join(d["title"] for d in docs)
        kw_score, hits = score_text(combined)
        # Artifact diversity bonus
        artifact_bonus = (4 if has_mp3 else 0) + (3 if has_video else 0) + (1 if has_pdf else 0)
        total_downloads = sum(d["downloads"] for d in docs)
        candidates.append({
            "source": "sfdpa",
            "case_id": f"sfdpa_{folder}",
            "folder": folder,
            "n_docs": len(docs),
            "has_mp3": has_mp3,
            "has_video": has_video,
            "has_pdf": has_pdf,
            "total_downloads": total_downloads,
            "titles": [d["title"] for d in docs],
            "urls": [d["url"] for d in docs],
            "keyword_score": kw_score,
            "artifact_bonus": artifact_bonus,
            "score": kw_score + artifact_bonus + min(total_downloads // 100, 3),
            "hits": hits,
        })
    return candidates


def parse_cib_playlists() -> list:
    """Parse the 3 CIB playlists into candidates."""
    data = json.load(open(ROOT / "cib_playlists.json", encoding="utf-8"))
    candidates = []
    for playlist_name, videos in data.items():
        for v in videos:
            title = v.get("title", "")
            if not title:
                continue
            kw_score, hits = score_text(title)
            # Duration signal: 3-15 min is ideal (CIBs are usually 5-10 min)
            dur = v.get("duration") or 0
            dur_min = dur / 60 if dur else 0
            duration_bonus = 2 if 3 <= dur_min <= 18 else 0
            candidates.append({
                "source": f"cib_{playlist_name}",
                "case_id": f"{playlist_name}_{v.get('id')}",
                "title": title,
                "url": v.get("url"),
                "duration_sec": dur,
                "duration_min": round(dur_min, 1),
                "has_video": True,
                "has_mp3": False,
                "has_pdf": False,
                "keyword_score": kw_score,
                "artifact_bonus": 3 + duration_bonus,
                "score": kw_score + 3 + duration_bonus,
                "hits": hits,
            })
    return candidates


def main():
    sfdpa = parse_sfdpa_docs()
    cib = parse_cib_playlists()
    all_candidates = sfdpa + cib

    # Drop obvious non-narrative entries
    all_candidates = [c for c in all_candidates if c["score"] > -3]

    # Sort by score desc
    all_candidates.sort(key=lambda c: c["score"], reverse=True)

    # Write full ranked list
    with open(ROOT / "candidates_ranked.json", "w", encoding="utf-8") as f:
        json.dump(all_candidates, f, indent=2, ensure_ascii=False)

    # Print top 15 for inspection
    print(f"Total candidates: {len(all_candidates)}")
    print(f"  SF DPA folders: {len(sfdpa)}")
    print(f"  CIB videos:     {len(cib)}")
    print()
    print("=" * 80)
    print("TOP 15 CANDIDATES")
    print("=" * 80)
    for i, c in enumerate(all_candidates[:15], 1):
        src = c["source"]
        if src == "sfdpa":
            label = f"{c['case_id']} ({c['n_docs']} docs, mp3={c['has_mp3']}, dl={c['total_downloads']})"
        else:
            label = f"{c['case_id']} ({c['duration_min']}min) — {c.get('title', '')[:60]}"
        print(f"{i:2}. [{c['score']:3}] {label}")
        print(f"     hits: {', '.join(c['hits'][:5])}")


if __name__ == "__main__":
    main()
