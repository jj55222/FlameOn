"""P3.5 / GOAL_D — LLM document extractor (the any-format fallback).

``doc_extract.py`` dispatches on two known templates (Blueteam UoF form, IA
narrative). Real case packets — an OIS investigation, a DA letter, interview
transcripts — match neither, so this reads ANY document text and returns the
same structured shape, via an LLM. It is the generalization that lets the engine
ingest an arbitrary FOIA bundle instead of one tuned agency form.

Grounded extraction only: the model reports what the document SAYS (disposition,
subjects, officers, incident time/place, force facts, the document's own
pointers to footage), never invents. Long documents are chunked and merged.

Pure parse/merge helpers are zero-network testable; ``LLMDocBackend`` (OpenRouter)
is the only paid part, with ``MockDocBackend`` for tests/dry-runs.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Same outward shape as doc_extract.extract(), so downstream (blueprint, clip
# directions) consumes it identically.
_FIELDS = ("doc_type", "case_number", "incident_date", "incident_time",
           "location", "subjects", "officers", "summary", "outcome",
           "force_facts", "charges", "clip_directions", "key_quotes")


def _strip_fences(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^```(?:json)?\s*\n?", "", s)
    s = re.sub(r"\n?```\s*$", "", s).strip()
    a, b = s.find("{"), s.rfind("}")
    return s[a:b + 1] if (a != -1 and b > a) else s


def parse_doc(raw: str) -> Dict:
    """Parse one LLM reply into a partial case dict (tolerant, fence-stripped)."""
    try:
        d = json.loads(_strip_fences(raw)) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(d, dict):
        return {}
    out: Dict[str, Any] = {}
    for k in _FIELDS:
        if k in d and d[k] not in (None, "", []):
            out[k] = d[k]
    return out


def _merge(parts: List[Dict]) -> Dict:
    """Merge per-chunk extractions: scalars take the first non-empty, lists union
    (deduped), so a fact found on any page survives."""
    merged: Dict[str, Any] = {}
    for p in parts:
        for k, v in p.items():
            if isinstance(v, list):
                cur = merged.setdefault(k, [])
                for item in v:
                    key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
                    if key not in {json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x) for x in cur}:
                        cur.append(item)
            elif k not in merged or merged[k] in (None, "", []):
                merged[k] = v
    return merged


def chunk_text(text: str, max_chars: int = 28000, overlap: int = 800) -> List[str]:
    """Split long document text into overlapping chunks (pure)."""
    text = text or ""
    if len(text) <= max_chars:
        return [text] if text.strip() else []
    out, i = [], 0
    while i < len(text):
        out.append(text[i:i + max_chars])
        i += max_chars - overlap
    return out


_SYSTEM = (
    "You are a paralegal extracting STRUCTURED FACTS from a police case document "
    "(could be an investigation report, DA letter, use-of-force report, or an "
    "interview transcript). Report ONLY what the text states — never infer or "
    "invent. If a field isn't in this chunk, omit it. Return ONLY JSON with any of: "
    '{"doc_type":"investigation|da_letter|uof|interview|other","case_number":"..",'
    '"incident_date":"..","incident_time":"..","location":"..","subjects":["the '
    'civilian(s) involved/harmed"],"officers":["names"],"summary":"2-3 sentences of '
    'what happened","outcome":"disposition / DA decision / discipline","force_facts":'
    '["e.g. N rounds fired, weapon, who fired"],"charges":["PC.."],"clip_directions":'
    '[{"ref":"camera/witness","window":"time or page","note":".."}],"key_quotes":'
    '[{"speaker":"..","text":"verbatim significant line"}]}'
)


class MockDocBackend:
    def __init__(self, reply: Any = None):
        self._r = reply if reply is not None else {}
        self.calls: List[str] = []

    def complete(self, system: str, user: str, **kw) -> str:
        self.calls.append(user)
        return self._r if isinstance(self._r, str) else json.dumps(self._r)


class LLMDocBackend:
    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(self, model: str = "google/gemini-3.1-flash-lite-preview",
                 api_key: Optional[str] = None, timeout: int = 240):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY not set (load .env)")
        self.timeout = timeout
        self._c = None

    def complete(self, system: str, user: str, max_tokens: int = 1500,
                 temperature: float = 0.1) -> str:
        if self._c is None:
            from openai import OpenAI  # type: ignore
            self._c = OpenAI(api_key=self.api_key, base_url=self.BASE_URL)
        r = self._c.chat.completions.create(
            model=self.model, temperature=temperature, max_tokens=max_tokens, timeout=self.timeout,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            extra_headers={"HTTP-Referer": "https://github.com/jj55222/FlameOn", "X-Title": "FlameOn DocExtract"})
        return r.choices[0].message.content or ""


def extract_case(text: str, backend, max_chunks: int = 8) -> Dict:
    """Extract structured case fields from document text (chunked + merged)."""
    chunks = chunk_text(text)[:max_chunks]
    parts = [parse_doc(backend.complete(system=_SYSTEM, user="DOCUMENT:\n" + c)) for c in chunks]
    out = _merge([p for p in parts if p])
    out.setdefault("doc_type", "other")
    out["_chunks"] = len(chunks)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _pdf_text(path: Path) -> str:
    from pypdf import PdfReader  # type: ignore
    return "\n".join((p.extract_text() or "") for p in PdfReader(str(path)).pages)


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser(description="GOAL_D — LLM doc extractor (any format)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--pdf", type=Path, help="extract text from a PDF then mine it")
    src.add_argument("--text", type=Path, help="a .txt file")
    ap.add_argument("--model", default=None)
    ap.add_argument("--max-chunks", type=int, default=8)
    ap.add_argument("--mock", action="store_true")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)

    text = _pdf_text(args.pdf) if args.pdf else args.text.read_text(encoding="utf-8", errors="ignore")
    print(f"[doc-llm] {len(text)} chars, ~{len(chunk_text(text))} chunk(s)")
    if args.mock:
        backend = MockDocBackend()
    else:
        try:
            from dotenv import load_dotenv  # type: ignore
            load_dotenv(Path(__file__).resolve().parent.parent / ".env"); load_dotenv()
        except Exception:
            pass
        backend = LLMDocBackend(args.model) if args.model else LLMDocBackend()
        print(f"[doc-llm] live: {backend.model} (paid)")

    case = extract_case(text, backend, max_chunks=args.max_chunks)
    print(json.dumps({k: v for k, v in case.items() if k != "key_quotes"}, indent=2, ensure_ascii=False)[:1800])
    if case.get("key_quotes"):
        print("KEY QUOTES:")
        for q in case["key_quotes"][:6]:
            print(f"  [{q.get('speaker','?')}] {str(q.get('text',''))[:90]}")
    out = args.out or (args.pdf or args.text).with_suffix(".case.json")
    out.write_text(json.dumps(case, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[doc-llm] -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
