"""
MuckRock harvester — a new case source for the FlameOn intake pipeline.

Mirrors the discovered_cases/ pattern (rank_candidates.py + download_all.py):
queries MuckRock's API for *completed* FOIA requests, walks each request's
communications down to its released files, keeps only requests that released
actual VIDEO or AUDIO media, scores them with the same narrative keyword
ranker used for the other sources, and emits a candidates list + a download
manifest in the schema pipeline3_audio already understands.

API chain (MuckRock api_v2):
    requests/?status=done&search=<term>      -> matching FOIA requests
    communications/?request=<id>             -> each request's comms
        comm["files"]                        -> released files (ffile = CDN url)
  (fallback) files/?communication=<comm_id>  -> files for a comm

AUTH: api_v2 uses Squarelet JWT (SimpleJWT). There is NO static API token.
POST your muckrock.com username/password to
    https://accounts.muckrock.com/api/token/   -> {access, refresh}
then call api_v2 with `Authorization: Bearer <access>`. Put creds in .env as
MUCKROCK_USERNAME / MUCKROCK_PASSWORD (or supply a pre-obtained access JWT as
MUCKROCK_ACCESS_TOKEN). Without them this script exits early — it cannot probe
anything anonymously (bare api_v2 calls 401).

NOTE: built against MuckRock's documented api_v2 schema but NOT yet run against
the live API (no token at authoring time). Field access is defensive; if a
field name differs, the offending request is skipped (not crashed) and counted
in the run summary. Verify the first live run against ONE known request.

Usage:
    export MUCKROCK_API_TOKEN=...        # or set in .env
    python discovered_cases/muckrock_harvest.py                # harvest + rank
    python discovered_cases/muckrock_harvest.py --download     # also fetch media
    python discovered_cases/muckrock_harvest.py --terms "officer involved shooting" "in custody death"
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

# Reuse the exact narrative keyword scorer the other sources rank with.
sys.path.insert(0, str(Path(__file__).parent))
from rank_candidates import score_text  # noqa: E402

ROOT = Path(__file__).parent
API_BASE = "https://www.muckrock.com/api_v2/"
# MuckRock api_v2 uses Squarelet JWT auth (SimpleJWT TokenObtainPair): POST
# username/password here -> {access, refresh}, then call api_v2 with
# `Authorization: Bearer <access>`. There is no static API token.
TOKEN_URL = "https://accounts.muckrock.com/api/token/"
REQUEST_TIMEOUT = 30
RATE_LIMIT_SEC = 0.4  # polite but not glacial; files are embedded so ~1 call/request

# Default search net, TUNED against live MuckRock yield (calibrate_terms probe,
# 2026-06-27). Lesson: media-EXPLICIT phrasings ("dash camera", "body camera
# footage", "police video") surface actual video; generic policy/event terms
# ("use of force", "in custody death", "bodycam", "critical incident") return
# only documents and were dropped. Re-run scratchpad/calibrate_terms.py to
# re-tune if MuckRock's corpus shifts.
DEFAULT_TERMS = [
    # proven video-yielding (top of the calibration table)
    "dash camera",
    "dash cam",
    "in-car video",
    "body camera footage",
    "bodycam footage",
    "body camera video",
    "police video",
    "surveillance video",
    "officer involved shooting",
    "officer-involved shooting",
    "police shooting video",
    "taser",
]

VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".m4v", ".wmv", ".flv", ".webm", ".mpg", ".mpeg"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".wma"}
DOC_EXTS = {".pdf", ".doc", ".docx", ".txt", ".rtf"}

# Artifact signals sniffed from request + file titles. The goal is no longer
# "video only" — a SB1421/16 accountability release or an interrogation record
# is a desirable case artifact too. The ideal "winner" bundles several.
RE_SB16 = re.compile(
    r"\b(sb[\s.-]?1421|sb[\s.-]?16\b|senate bill 1421|penal code 832\.7|832\.7"
    r"|peace officer.{0,20}record|personnel record|misconduct record"
    r"|use[\s-]?of[\s-]?force report)", re.I)
RE_INTERR = re.compile(r"\b(interrogat|custodial interview|suspect interview|interview of)", re.I)
RE_BWC = re.compile(r"\b(body[\s-]?worn|bwc|body[\s-]?cam|axon|in[\s-]?car video|dash[\s-]?cam)", re.I)


def detect_artifacts(text: str) -> Dict[str, bool]:
    return {
        "sb16": bool(RE_SB16.search(text)),
        "interrogation": bool(RE_INTERR.search(text)),
        "bwc": bool(RE_BWC.search(text)),
    }


def _read_env_file() -> Dict[str, str]:
    """Minimal .env parser: KEY=value, strips quotes + inline comments."""
    out: Dict[str, str] = {}
    env = ROOT.parent / ".env"
    if not env.exists():
        return out
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key, val = key.strip(), val.strip()
        if val[:1] not in ("'", '"') and "#" in val:
            val = val.split("#", 1)[0].strip()
        out[key] = val.strip('"').strip("'").strip()
    return out


def load_credentials() -> Dict[str, str]:
    """Squarelet creds (or a pre-obtained access JWT) from env, then .env."""
    file_env = _read_env_file()

    def pick(*keys: str) -> str:
        for k in keys:
            v = os.environ.get(k, "").strip() or file_env.get(k, "").strip()
            if v:
                return v
        return ""

    return {
        "username": pick("MUCKROCK_USERNAME", "MUCKROCK_USER"),
        "password": pick("MUCKROCK_PASSWORD", "MUCKROCK_PASS"),
        # Optional: skip the username/password exchange by supplying an access JWT.
        "access": pick("MUCKROCK_ACCESS_TOKEN"),
    }


class AuthError(RuntimeError):
    pass


class MuckRock:
    def __init__(self, creds: Dict[str, str]):
        self._creds = creds
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/json",
            "User-Agent": "FlameOn-intake/1.0",
        })
        self._last = 0.0
        self._authenticate()

    def _authenticate(self) -> None:
        """Obtain (or reuse) a Bearer access token for api_v2."""
        access = self._creds.get("access")
        if not access:
            user, pw = self._creds.get("username"), self._creds.get("password")
            if not (user and pw):
                raise AuthError("need MUCKROCK_USERNAME + MUCKROCK_PASSWORD "
                                "(or MUCKROCK_ACCESS_TOKEN)")
            resp = self.s.post(TOKEN_URL, json={"username": user, "password": pw},
                               timeout=REQUEST_TIMEOUT)
            if resp.status_code == 401:
                raise AuthError("MuckRock rejected credentials (401)")
            resp.raise_for_status()
            tok = resp.json()
            access = tok.get("access")
            self._creds["refresh"] = tok.get("refresh", "")
            if not access:
                raise AuthError(f"token endpoint returned no access token: {tok}")
        self.s.headers["Authorization"] = f"Bearer {access}"

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None,
             _retried: bool = False) -> Optional[dict]:
        wait = RATE_LIMIT_SEC - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        try:
            r = self.s.get(url, params=params, timeout=REQUEST_TIMEOUT)
            self._last = time.monotonic()
            if r.status_code == 401 and not _retried and not self._creds.get("access"):
                # Access JWT likely expired mid-run — re-auth once and retry.
                print("  [401] re-authenticating ...", file=sys.stderr)
                self._authenticate()
                return self._get(url, params, _retried=True)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001 — log + skip, never crash the harvest
            print(f"  [warn] GET {url} failed: {e}", file=sys.stderr)
            return None

    def paginate(self, url: str, params: Dict[str, Any], cap: int) -> Iterable[dict]:
        """Yield result records across pages until `cap` reached or exhausted."""
        seen = 0
        next_url: Optional[str] = url
        next_params: Optional[Dict[str, Any]] = dict(params)
        while next_url and seen < cap:
            data = self._get(next_url, next_params)
            if not data:
                return
            for rec in data.get("results", []):
                yield rec
                seen += 1
                if seen >= cap:
                    return
            next_url = data.get("next")
            next_params = None  # `next` is a fully-formed URL

    def agency_name(self, agency_id: Any) -> str:
        """Resolve an agency id -> 'Name (Jurisdiction)' for the cross-source pivot.

        The requests serializer returns `agency` as a bare int id; the portal
        search downstream needs the human name + jurisdiction. Cached per run.
        """
        if not isinstance(agency_id, int):
            return str(agency_id or "")
        cache = getattr(self, "_agency_cache", None)
        if cache is None:
            cache = self._agency_cache = {}
        if agency_id in cache:
            return cache[agency_id]
        d = self._get(f"{API_BASE}agencies/{agency_id}/") or {}
        name = d.get("name") or ""
        juris = d.get("jurisdiction")
        if isinstance(juris, dict):
            name = f"{name} ({juris.get('name')})" if juris.get("name") else name
        cache[agency_id] = name or str(agency_id)
        return cache[agency_id]

    def search_requests(self, term: str, status: str, cap: int) -> List[dict]:
        return list(self.paginate(
            f"{API_BASE}requests/",
            {"search": term, "status": status, "page_size": 50},
            cap,
        ))

    def files_for_request(self, req: dict, file_cap: int = 200) -> List[dict]:
        """Collect released file records for a request.

        Two MuckRock api_v2 gotchas baked in here:
          * The communications endpoint filters on `foia=<request_id>`. The
            intuitive `request=` is SILENTLY IGNORED and returns the global feed
            (~1.4M comms) — so it must be `foia`.
          * A communication's `files` field is a list of integer file IDs, not
            nested objects. To get real records (with the `ffile` download url)
            we resolve per-communication via `files/?communication=<id>`, which
            is the one files filter that actually works (`id__in`/`request` are
            ignored and dump the global 1M-file feed).
        """
        req_id = req.get("id")
        if req_id is None:
            return []
        comms = list(self.paginate(
            f"{API_BASE}communications/",
            {"foia": req_id, "page_size": 50},
            cap=400,
        ))
        files: List[dict] = []
        for comm in comms:
            if not comm.get("files"):  # empty/absent => no attachments on this comm
                continue
            comm_id = comm.get("id")
            if comm_id is None:
                continue
            files.extend(self.paginate(
                f"{API_BASE}files/",
                {"communication": comm_id, "page_size": 50},
                cap=file_cap,
            ))
            if len(files) >= file_cap:
                break
        return files


def file_url(f: dict) -> str:
    """The downloadable CDN url. MuckRock exposes this as `ffile`."""
    return f.get("ffile") or f.get("file") or f.get("url") or ""


def file_name(f: dict) -> str:
    name = f.get("title") or f.get("name") or ""
    if not name:
        # derive from the url path
        u = file_url(f)
        name = u.rsplit("/", 1)[-1].split("?", 1)[0] if u else ""
    return name


def ext_of(name_or_url: str) -> str:
    base = name_or_url.split("?", 1)[0].rsplit("/", 1)[-1]
    m = re.search(r"(\.[A-Za-z0-9]{1,5})$", base)
    return m.group(1).lower() if m else ""


def classify(files: List[dict]) -> Dict[str, Any]:
    vids, auds, docs, others = [], [], [], []
    for f in files:
        name = file_name(f)
        url = file_url(f)
        e = ext_of(name) or ext_of(url)
        rec = {"name": name, "url": url, "ext": e}
        if e in VIDEO_EXTS:
            vids.append(rec)
        elif e in AUDIO_EXTS:
            auds.append(rec)
        elif e in DOC_EXTS:
            docs.append(rec)
        else:
            others.append(rec)
    return {"video": vids, "audio": auds, "doc": docs, "other": others}


def slugify(s: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", (s or "").lower()).strip("_")
    return s[:maxlen] or "untitled"


def make_candidate(req: dict, buckets: Dict[str, Any], agency_str: str = "") -> Dict[str, Any]:
    """Emit a record in the discovered_cases candidates schema (+ muckrock fields).

    Scoring is artifact-bundle aware: a case is valued for the MIX it carries
    (BWC/dash video, interrogation/interview, SB1421/16 accountability docs),
    with a bonus when several co-occur in one request — the ideal "winner".
    """
    title = req.get("title", "") or ""
    if not agency_str:
        agency_str = str(req.get("agency") or "")
    all_names = [b["name"] for k in ("video", "audio", "doc", "other") for b in buckets[k]]
    blob = f"{title} {' '.join(all_names)}"
    keyword_score, hits = score_text(blob)
    art = detect_artifacts(blob)

    n_vid, n_aud, n_doc = len(buckets["video"]), len(buckets["audio"]), len(buckets["doc"])
    # An accountability/interrogation document is itself a desirable artifact.
    sb16_doc = art["sb16"] and n_doc > 0
    interr_doc = art["interrogation"] and (n_doc > 0 or n_aud > 0 or n_vid > 0)

    artifact_bonus = (
        (6 if n_vid else 0)
        + (5 if n_aud else 0)
        + (5 if sb16_doc else 0)
        + (4 if interr_doc else 0)
        + (1 if n_doc else 0)
    )
    # Bundle bonus: the ideal is one case carrying several artifact kinds.
    kinds = sum([bool(n_vid), bool(n_aud), bool(sb16_doc), bool(interr_doc)])
    bundle_bonus = {0: 0, 1: 0, 2: 3, 3: 7, 4: 12}[kinds]

    foia_id = req.get("id")
    return {
        "source": "muckrock",
        "case_id": f"muckrock_{foia_id}_{slugify(title, 40)}",
        "foia_id": foia_id,
        "title": title,
        "agency": agency_str,
        "absolute_url": req.get("absolute_url", ""),
        "datetime_done": req.get("datetime_done") or req.get("date_done"),
        "n_docs": n_doc,
        "n_video": n_vid,
        "n_audio": n_aud,
        "has_video": bool(n_vid),
        "has_mp3": bool(n_aud),
        "has_pdf": bool(n_doc),
        # artifact flags drive both the keep-filter and the cross-source pivot
        "is_bwc": art["bwc"] or bool(n_vid),
        "is_interrogation": interr_doc,
        "is_sb16": sb16_doc,
        "artifact_kinds": kinds,
        # what the agency portal pivot would search for (SB16 release etc.)
        "pivot": {"agency": agency_str, "case_title": title,
                  "done": req.get("datetime_done") or req.get("date_done")},
        "titles": all_names[:25],
        "media_files": buckets["video"] + buckets["audio"],  # download targets
        "doc_files": buckets["doc"][:25],
        "urls": [f["url"] for f in (buckets["video"] + buckets["audio"]) if f["url"]],
        "keyword_score": keyword_score,
        "artifact_bonus": artifact_bonus,
        "bundle_bonus": bundle_bonus,
        "score": keyword_score + artifact_bonus + bundle_bonus,
        "hits": hits,
    }


def download_media(candidates: List[dict], cache_dir: Path, session: requests.Session) -> dict:
    """Fetch each candidate's media into cache_dir. Returns a manifest."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest: Dict[str, List] = {}
    for c in candidates:
        entries = []
        for i, f in enumerate(c["media_files"]):
            url = f["url"]
            if not url:
                continue
            ext = f["ext"] or ".bin"
            dest = cache_dir / f"{c['case_id']}_{i}{ext}"
            if dest.exists() and dest.stat().st_size > 0:
                entries.append([f["name"], str(dest), "cached"])
                continue
            try:
                print(f"  [dl] {c['case_id']} <- {url[:80]}")
                with session.get(url, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    with open(dest, "wb") as fh:
                        for chunk in r.iter_content(1 << 20):
                            fh.write(chunk)
                entries.append([f["name"], str(dest), "downloaded"])
            except Exception as e:  # noqa: BLE001
                print(f"  [dl-fail] {url[:80]}: {e}", file=sys.stderr)
                entries.append([f["name"], url, f"failed: {e}"])
        manifest[c["case_id"]] = entries
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--terms", nargs="*", default=DEFAULT_TERMS, help="search terms")
    ap.add_argument("--status", default="done", help="FOIA status filter (default: done)")
    ap.add_argument("--per-term", type=int, default=40, help="max requests fetched per term")
    ap.add_argument("--min-score", type=int, default=3, help="drop candidates below this score")
    ap.add_argument("--keep", choices=["artifact", "media", "any"], default="artifact",
                    help="keep rule: 'artifact' (video/audio/SB16-doc/interrogation — default), "
                         "'media' (video/audio only), 'any' (anything with files)")
    ap.add_argument("--download", action="store_true", help="download media into the cache dir")
    ap.add_argument("--cache-dir", default=str(ROOT.parent / "pipeline3_audio" / "foia_cache"))
    ap.add_argument("--out", default=str(ROOT / "muckrock_candidates.json"))
    args = ap.parse_args()

    creds = load_credentials()
    try:
        mr = MuckRock(creds)
    except AuthError as e:
        print(f"ERROR: MuckRock auth failed: {e}\n"
              "Set MUCKROCK_USERNAME and MUCKROCK_PASSWORD (your muckrock.com login) "
              "in .env, then re-run. api_v2 uses Squarelet JWT auth — there is no "
              "static API token.", file=sys.stderr)
        return 2

    seen_ids: set = set()
    candidates: List[dict] = []
    stats = {"requests_seen": 0, "no_files": 0, "filtered": 0, "kept": 0}

    for term in args.terms:
        print(f"[search] {term!r}")
        for req in mr.search_requests(term, args.status, args.per_term):
            rid = req.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            stats["requests_seen"] += 1

            files = mr.files_for_request(req)
            if not files:
                stats["no_files"] += 1
                continue
            buckets = classify(files)
            cand = make_candidate(req, buckets, mr.agency_name(req.get("agency")))

            has_media = cand["has_video"] or cand["has_mp3"]
            has_artifact = has_media or cand["is_sb16"] or cand["is_interrogation"]
            keep = {"media": has_media, "artifact": has_artifact, "any": True}[args.keep]
            if not keep or cand["score"] < args.min_score:
                stats["filtered"] += 1
                continue

            candidates.append(cand)
            stats["kept"] += 1
            tags = "".join(t for t, on in (("V", cand["has_video"]), ("A", cand["has_mp3"]),
                          ("S", cand["is_sb16"]), ("I", cand["is_interrogation"])) if on)
            print(f"  [keep {cand['score']:>3} {tags:<4}] {cand['case_id']}  "
                  f"(vid={cand['n_video']} aud={cand['n_audio']} doc={cand['n_docs']})")

    candidates.sort(key=lambda c: c["score"], reverse=True)

    out = Path(args.out)
    out.write_text(json.dumps(candidates, indent=2, ensure_ascii=False))

    print("\n" + "=" * 72)
    print(f"requests seen={stats['requests_seen']}  no-files={stats['no_files']}  "
          f"doc-only-skipped={stats['doc_only']}  kept={stats['kept']}")
    print(f"wrote {len(candidates)} candidates -> {out}")
    print("=" * 72)
    for i, c in enumerate(candidates[:15], 1):
        print(f"{i:2}. [{c['score']:3}] {c['case_id']}  "
              f"vid={c['n_video']} aud={c['n_audio']}")
        print(f"     {c['title'][:70]}")
        if c["hits"]:
            print(f"     hits: {', '.join(c['hits'][:6])}")

    if args.download and candidates:
        print("\n[download] fetching media ...")
        manifest = download_media(candidates, Path(args.cache_dir), mr.s)
        man_path = ROOT / "muckrock_download_manifest.json"
        man_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
        print(f"[download] manifest -> {man_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
