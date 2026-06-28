"""Court-doc username OSINT harness.

This is a lightweight enrichment pass for documentary research. It turns names,
email prefixes, and explicit handles found in court / investigation documents
into an IntelTechniques-style username review pack. It does not log in to
platforms, scrape private pages, contact subjects, or claim that a profile is
the right person. Output is intentionally framed as leads to corroborate.

Examples:
  python pipeline2_discovery/osint_username_harness.py --text case.txt --out .tmp/osint/case
  python pipeline2_discovery/osint_username_harness.py --pdf report.pdf --doc-extract report.case.json --out .tmp/osint/report
  python pipeline2_discovery/osint_username_harness.py --pages ocr/pages.json --bundle-id longbeach_laserfiche:longbeach_... --out .tmp/osint/lbpd
"""
from __future__ import annotations

import argparse
import json
import re
import urllib.parse
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
AGG_PATH = REPO_ROOT / "discovered_cases" / "CASE_BUNDLE_AGG.json"

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
HANDLE_RE = re.compile(r"(?<![\w.])@([A-Za-z][A-Za-z0-9_.]{2,29})\b")
REDDIT_RE = re.compile(r"\b(?:reddit(?:\s+user(?:name)?)?|u/)\s*[:=]?\s*/?u?/([A-Za-z0-9_-]{3,24})\b", re.I)
PLATFORM_HANDLE_RE = re.compile(
    r"\b(Instagram|IG|Reddit|TikTok|Twitter|X|Facebook|YouTube|Snapchat|Discord)\b"
    r"\s*(?:handle|username|user|account|profile)?\s*[:=]\s*@?([A-Za-z][A-Za-z0-9_.-]{2,30})",
    re.I,
)
TITLE_NAME_RE = re.compile(
    r"\b(?:Officer|Deputy|Detective|Sergeant|Sgt\.?|Trooper|Agent|Defendant|Victim|Witness|Suspect|Subject)"
    r"\s+([A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3})\b"
)
LABELED_NAME_RE = re.compile(
    r"\b(?:Defendant|Victim|Witness|Suspect|Subject|Decedent|Arrestee|Officer|Deputy|Detective)"
    r"\s*(?:Name)?\s*[:=]\s*([A-Z][a-zA-Z'.-]+(?:\s+[A-Z][a-zA-Z'.-]+){1,3})\b"
)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
CASE_NOISE_RE = re.compile(r"\b(?:case|report|incident|cad|event|uof|ois|adm|icd|psb|dr|lbpd|pdf|redacted)\b", re.I)
MINOR_RE = re.compile(r"\b(?:minor|juvenile|child victim|under\s+18|DOB\s*[:=]?\s*\d{1,2}/\d{1,2}/\d{2,4})\b", re.I)

STOP_NAME_PARTS = {
    "city",
    "county",
    "police",
    "department",
    "superior",
    "court",
    "state",
    "people",
    "officer",
    "deputy",
    "detective",
    "sergeant",
    "victim",
    "witness",
    "suspect",
    "defendant",
}


@dataclass
class Entity:
    name: str
    role: str
    source: str


@dataclass
class Seed:
    value: str
    seed_type: str
    source: str
    confidence: int
    entity: str = ""


@dataclass
class UsernameCandidate:
    username: str
    source_seed: str
    seed_type: str
    entity: str
    confidence: int
    rationale: str
    links: Dict[str, str] = field(default_factory=dict)


def normalize_spaces(text: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", text or "").strip()


def load_text(*, text_path: Optional[Path], pages_path: Optional[Path], pdf_path: Optional[Path]) -> str:
    if text_path:
        return text_path.read_text(encoding="utf-8", errors="ignore")
    if pages_path:
        data = json.loads(pages_path.read_text(encoding="utf-8"))
        pages = data.get("pages", data) if isinstance(data, dict) else data
        if not isinstance(pages, list):
            raise ValueError("--pages must be a list or {'pages': [...]}")
        return "\n".join(str(p.get("text", "")) for p in pages if isinstance(p, dict))
    if pdf_path:
        try:
            from pypdf import PdfReader  # type: ignore
        except Exception as exc:  # pragma: no cover - dependency path
            raise RuntimeError("PDF input requires pypdf; use --text or --pages if unavailable") from exc
        return "\n".join((p.extract_text() or "") for p in PdfReader(str(pdf_path)).pages)
    raise ValueError("one of --text, --pages, or --pdf is required")


def load_doc_extract(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def bundle_context(bundle_id: str, agg_path: Path = AGG_PATH) -> Dict[str, Any]:
    if not bundle_id:
        return {}
    source, sep, case_id = bundle_id.partition(":")
    if not sep:
        case_id = source
        source = ""
    data = json.loads(agg_path.read_text(encoding="utf-8"))
    for b in data.get("bundles", []):
        if source and b.get("source") != source:
            continue
        if b.get("case_id") == case_id:
            return {
                "source": b.get("source"),
                "case_id": b.get("case_id"),
                "agency": b.get("agency"),
                "title": b.get("title"),
                "case_url": b.get("case_url"),
                "origin_file": b.get("origin_file"),
            }
    return {"requested_bundle_id": bundle_id, "warning": "bundle not found"}


def clean_handle(value: str) -> str:
    value = value.strip().strip("@").strip(".,;:()[]{}<>\"'")
    value = value.replace(" ", "")
    return value[:40]


def valid_username(value: str) -> bool:
    if not (3 <= len(value) <= 40):
        return False
    if CASE_NOISE_RE.search(value):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]+", value))


def normalize_name(name: str) -> str:
    name = re.sub(r"\b(?:Jr|Sr|II|III|IV)\.?\b", "", name)
    name = re.sub(r"[^A-Za-z' .-]", " ", name)
    parts = [p.strip(" .'\"-") for p in name.split() if p.strip(" .'\"-")]
    parts = [p for p in parts if p.lower() not in STOP_NAME_PARTS]
    if len(parts) < 2:
        return ""
    return " ".join(p[:1].upper() + p[1:] for p in parts[:4])


def extract_entities(text: str, doc_extract: Dict[str, Any]) -> List[Entity]:
    found: Dict[str, Entity] = {}

    def add(name: str, role: str, source: str) -> None:
        clean = normalize_name(name)
        if not clean:
            return
        key = clean.lower()
        found.setdefault(key, Entity(clean, role, source))

    for field, role in (
        ("subject", "subject"),
        ("subjects", "subject"),
        ("officers", "officer"),
        ("people", "person"),
        ("victim_names", "victim"),
        ("defendant_names", "defendant"),
    ):
        value = doc_extract.get(field)
        if isinstance(value, str):
            add(value, role, f"doc_extract.{field}")
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    add(item, role, f"doc_extract.{field}")
                elif isinstance(item, dict):
                    add(str(item.get("name") or item.get("text") or ""), role, f"doc_extract.{field}")

    for m in LABELED_NAME_RE.finditer(text):
        label = re.split(r"[:=]", m.group(0), 1)[0].strip().split()[0].lower()
        add(m.group(1), label, "labeled court-doc text")
    for m in TITLE_NAME_RE.finditer(text):
        prefix = m.group(0).split()[0].lower().rstrip(".")
        role = "officer" if prefix in {"officer", "deputy", "detective", "sergeant", "sgt", "trooper", "agent"} else prefix
        add(m.group(1), role, "title-name court-doc text")

    return sorted(found.values(), key=lambda e: (e.role, e.name))


def extract_seeds(text: str, entities: Sequence[Entity]) -> List[Seed]:
    seeds: Dict[tuple[str, str, str], Seed] = {}

    def add(value: str, seed_type: str, source: str, confidence: int, entity: str = "") -> None:
        value = clean_handle(value)
        if not valid_username(value):
            return
        key = (value.lower(), seed_type, entity.lower())
        seeds.setdefault(key, Seed(value, seed_type, source, confidence, entity))

    for email in EMAIL_RE.findall(text):
        local = email.split("@", 1)[0]
        add(local, "email_prefix", f"email address {email}", 72)

    for handle in HANDLE_RE.findall(text):
        # Skip email domains already captured.
        if "." in handle and handle.lower().split(".", 1)[0] in {"gmail", "yahoo", "hotmail", "outlook"}:
            continue
        add(handle, "explicit_handle", "explicit @handle in document", 90)

    for m in REDDIT_RE.finditer(text):
        add(m.group(1), "explicit_reddit", "explicit Reddit/u handle in document", 92)

    for m in PLATFORM_HANDLE_RE.finditer(text):
        add(m.group(2), f"explicit_{m.group(1).lower()}", f"explicit {m.group(1)} account in document", 94)

    years = sorted(set(YEAR_RE.findall(text)))
    years = [y for y in years if 1930 <= int(y) <= 2030][-5:]
    for ent in entities:
        for username in name_variants(ent.name, years):
            add(username, "name_variant", f"name variant from {ent.source}", 38, ent.name)

    return sorted(seeds.values(), key=lambda s: (-s.confidence, s.seed_type, s.value.lower()))


def name_variants(name: str, years: Sequence[str]) -> List[str]:
    parts = [re.sub(r"[^A-Za-z0-9]", "", p).lower() for p in name.split()]
    parts = [p for p in parts if p]
    if len(parts) < 2:
        return []
    first, last = parts[0], parts[-1]
    middle = parts[1] if len(parts) > 2 else ""
    base = [
        first + last,
        f"{first}.{last}",
        f"{first}_{last}",
        first[0] + last,
        first + last[0],
    ]
    if middle:
        base.extend([first + middle[0] + last, first[0] + middle[0] + last])
    for y in years[-2:]:
        yy = y[-2:]
        base.extend([first + last + yy, first[0] + last + yy])
    return dedupe(base)


def dedupe(values: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for value in values:
        key = value.lower()
        if key not in seen:
            seen.add(key)
            out.append(value)
    return out


def username_links(username: str) -> Dict[str, str]:
    q = urllib.parse.quote(username)
    quoted = urllib.parse.quote(f'"{username}"')
    return {
        "inteltechniques_username_tool": "https://inteltechniques.com/tools/Username.html",
        "google_exact": f"https://www.google.com/search?q={quoted}",
        "bing_exact": f"https://www.bing.com/search?q={quoted}",
        "instagram": f"https://www.instagram.com/{q}/",
        "reddit_user": f"https://www.reddit.com/user/{q}",
        "reddit_search": f"https://www.google.com/search?q={urllib.parse.quote('site:reddit.com/user OR site:reddit.com/r ' + username)}",
        "x_twitter": f"https://x.com/{q}",
        "tiktok": f"https://www.tiktok.com/@{q}",
        "youtube": f"https://www.youtube.com/{q}",
        "facebook": f"https://www.facebook.com/{q}",
        "linktree": f"https://linktr.ee/{q}",
        "namecheck": f"https://namechk.com/namechk-plugin-search-results/?n={q}",
        "social_searcher": f"https://www.social-searcher.com/search-users/?ntw=&q6={q}",
    }


def build_candidates(seeds: Sequence[Seed], limit: int) -> List[UsernameCandidate]:
    candidates: List[UsernameCandidate] = []
    seen = set()
    for seed in seeds:
        key = seed.value.lower()
        if key in seen:
            continue
        seen.add(key)
        if seed.seed_type.startswith("explicit"):
            rationale = "Document appears to contain this handle directly; verify platform ownership and context."
        elif seed.seed_type == "email_prefix":
            rationale = "Email local-part often reappears as a public username; treat as a lead only."
        else:
            rationale = "Generated from a named person in the document; high false-positive risk until corroborated."
        candidates.append(
            UsernameCandidate(
                username=seed.value,
                source_seed=seed.source,
                seed_type=seed.seed_type,
                entity=seed.entity,
                confidence=seed.confidence,
                rationale=rationale,
                links=username_links(seed.value),
            )
        )
        if len(candidates) >= limit:
            break
    return candidates


def safety_flags(text: str, entities: Sequence[Entity]) -> List[str]:
    flags = [
        "Do not contact subjects, relatives, victims, witnesses, or account owners.",
        "Do not publish a handle unless identity is corroborated by multiple public-record anchors and editorial review.",
        "Keep private, sealed, juvenile, victim, witness, and medical details out of story artifacts.",
    ]
    if MINOR_RE.search(text):
        flags.append("Document appears to mention minors/juveniles or DOB data; suppress direct social-handle publication by default.")
    if any(e.role in {"victim", "witness"} for e in entities):
        flags.append("Victim/witness entities detected; use only for internal corroboration unless independently public and necessary.")
    return flags


def build_report(
    *,
    text: str,
    doc_extract: Dict[str, Any],
    bundle: Dict[str, Any],
    limit: int,
) -> Dict[str, Any]:
    text = normalize_spaces(text)
    entities = extract_entities(text, doc_extract)
    seeds = extract_seeds(text, entities)
    candidates = build_candidates(seeds, limit=limit)
    return {
        "what": "Public username OSINT review pack generated from court/investigation documents.",
        "mode": "IntelTechniques-style search launcher; no platform scraping, login, or private access.",
        "case_context": bundle,
        "entities": [asdict(e) for e in entities],
        "seed_count": len(seeds),
        "username_candidates": [asdict(c) for c in candidates],
        "manual_review": {
            "confirmed": "2-3 independent public anchors match the case/person.",
            "probable": "Handle plus at least one strong anchor match.",
            "possible": "Plausible username only; do not use editorially.",
            "discard": "No corroboration or wrong person.",
        },
        "safety_flags": safety_flags(text, entities),
    }


def write_markdown(report: Dict[str, Any], path: Path) -> None:
    lines: List[str] = [
        "# Username OSINT Review Pack",
        "",
        "> Generated from court/investigation documents. Leads are unverified until corroborated.",
        "",
    ]
    ctx = report.get("case_context") or {}
    if ctx:
        lines.extend(
            [
                "## Case Context",
                "",
                f"- Source: `{ctx.get('source', '')}`",
                f"- Case ID: `{ctx.get('case_id', ctx.get('requested_bundle_id', ''))}`",
                f"- Agency: {ctx.get('agency', '')}",
                f"- Title: {ctx.get('title', '')}",
                f"- Case URL: {ctx.get('case_url', '')}",
                "",
            ]
        )

    lines.extend(["## Entities", ""])
    entities = report.get("entities") or []
    if entities:
        for e in entities:
            lines.append(f"- **{e['name']}** — {e['role']} ({e['source']})")
    else:
        lines.append("- No named entities extracted.")

    lines.extend(
        [
            "",
            "## Username Leads",
            "",
            "| confidence | username | seed | entity | key links |",
            "|----------:|----------|------|--------|-----------|",
        ]
    )
    for c in report.get("username_candidates") or []:
        links = c["links"]
        key_links = (
            f"[Instagram]({links['instagram']}) / [Reddit]({links['reddit_user']}) / "
            f"[Google]({links['google_exact']}) / [IntelTechniques]({links['inteltechniques_username_tool']})"
        )
        lines.append(
            f"| {c['confidence']} | `{c['username']}` | {c['seed_type']} | "
            f"{c.get('entity') or ''} | {key_links} |"
        )

    lines.extend(["", "## Review Standard", ""])
    for k, v in (report.get("manual_review") or {}).items():
        lines.append(f"- **{k}:** {v}")

    lines.extend(["", "## Guardrails", ""])
    for flag in report.get("safety_flags") or []:
        lines.append(f"- {flag}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--text", type=Path, help="plain text court/investigation document")
    src.add_argument("--pages", type=Path, help="OCR pages JSON, list or {'pages': [...]}")
    src.add_argument("--pdf", type=Path, help="PDF input, requires pypdf")
    ap.add_argument("--doc-extract", type=Path, help="optional doc_extract/doc_extract_llm JSON")
    ap.add_argument("--bundle-id", default="", help="optional CASE_BUNDLE_AGG id: source:case_id")
    ap.add_argument("--limit", type=int, default=80, help="max username leads to emit")
    ap.add_argument("--out", type=Path, required=True, help="output prefix or .json path")
    args = ap.parse_args(argv)

    text = load_text(text_path=args.text, pages_path=args.pages, pdf_path=args.pdf)
    doc = load_doc_extract(args.doc_extract)
    bundle = bundle_context(args.bundle_id) if args.bundle_id else {}
    report = build_report(text=text, doc_extract=doc, bundle=bundle, limit=args.limit)

    out_json = args.out if args.out.suffix == ".json" else args.out.with_suffix(".json")
    out_md = out_json.with_suffix(".md")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    write_markdown(report, out_md)
    print(f"[osint] entities={len(report['entities'])} leads={len(report['username_candidates'])}")
    print(f"[osint] -> {out_json}")
    print(f"[osint] -> {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
