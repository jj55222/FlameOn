"""Zero-network tests for the LLM doc-extractor (the any-format fallback).

Pure parse/merge/chunk helpers + a MockDocBackend end-to-end. No PDF, no network.
"""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import doc_extract_llm as dl  # noqa: E402


# --- parse_doc --------------------------------------------------------------

def test_parse_doc_strips_fences_and_keeps_known_fields():
    raw = ('```json\n{"case_number":"17-289964","incident_date":"08/30/2017",'
           '"officers":["Peters"],"junk":"ignored","summary":""}\n```')
    d = dl.parse_doc(raw)
    assert d["case_number"] == "17-289964"
    assert d["officers"] == ["Peters"]
    assert "junk" not in d            # only known fields
    assert "summary" not in d         # empty omitted


def test_parse_doc_garbage_returns_empty():
    assert dl.parse_doc("not json") == {}
    assert dl.parse_doc("[1,2,3]") == {}


# --- merge ------------------------------------------------------------------

def test_merge_unions_lists_and_keeps_first_scalar():
    parts = [
        {"summary": "first", "force_facts": ["16 rounds"]},
        {"summary": "second", "force_facts": ["16 rounds", "two weapons"]},  # dup + new
        {"location": "Ramada room 324"},
    ]
    m = dl._merge(parts)
    assert m["summary"] == "first"                          # first non-empty scalar
    assert m["force_facts"] == ["16 rounds", "two weapons"] # union, deduped
    assert m["location"] == "Ramada room 324"


def test_merge_dedupes_dict_list_items():
    parts = [{"clip_directions": [{"ref": "BWC", "window": "12:00"}]},
             {"clip_directions": [{"ref": "BWC", "window": "12:00"}, {"ref": "dash"}]}]
    m = dl._merge(parts)
    assert len(m["clip_directions"]) == 2


# --- chunk ------------------------------------------------------------------

def test_chunk_short_text_single_chunk():
    assert dl.chunk_text("short doc") == ["short doc"]
    assert dl.chunk_text("") == []


def test_chunk_long_text_overlaps():
    text = "x" * 70000
    chunks = dl.chunk_text(text, max_chars=28000, overlap=800)
    assert len(chunks) >= 3
    assert all(len(c) <= 28000 for c in chunks)


# --- extract_case (mock) ----------------------------------------------------

def test_extract_case_merges_chunks_end_to_end():
    long_text = "A" * 60000   # forces multiple chunks
    backend = dl.MockDocBackend({"case_number": "17-289964",
                                 "force_facts": ["16 rounds"], "doc_type": "investigation"})
    case = dl.extract_case(long_text, backend)
    assert case["case_number"] == "17-289964"
    assert case["doc_type"] == "investigation"
    assert case["_chunks"] >= 2
    assert len(backend.calls) == case["_chunks"]    # one call per chunk


def test_extract_case_defaults_doc_type():
    case = dl.extract_case("some text", dl.MockDocBackend({"summary": "x"}))
    assert case["doc_type"] == "other"
