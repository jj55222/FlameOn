"""Tests for ``pipeline1_winners.packet_enricher``.

Covers the four public extractors and the orchestrator. Synthetic
fixtures only — no .tmp/ dependency. The fixtures intentionally mirror
the cached-caption shape we see from yt-dlp: lowercase, broken across
short lines, occasional misspellings.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from pipeline1_winners.packet_enricher import (  # noqa: E402
    augment_case_outcome_from_transcripts,
    enrich_packet,
    extract_case_outcome_from_urls,
    extract_family_decedent,
    extract_involved_officers,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


PACKET_HYLTON = {
    "packet_id": "pcs-test-022",
    "subject_or_case": "Karon Hylton-Brown — DC MPD scooter pursuit fatality, 2020-10-23 (officers Sutton + Zabevsky federally indicted/convicted)",
    "agency": "Metropolitan Police Department of the District of Columbia (MPDC)",
    "source_urls": [
        "https://www.justice.gov/usao-dc/pr/former-mpd-officers-convicted-death-20-year-old-karon-hylton-brown",
        "https://www.youtube.com/watch?v=y45SXhUH1Q0",
    ],
}

# Caption-style transcript: lowercase, fragments broken across short lines.
TRANSCRIPT_HYLTON_A = """
karon hilton brown's mother leading
protesters in the aftermath of her son's
death tonight a d.c police officer is
indicted for hilton brown's murder and
for obstruction of justice prosecutors
accuse officer terrence sutton of trying
to cover up this unauthorized chase
caught on body camera video the 20 year
old pursued while riding a scooter last
october until a crash on kennedy street
ended his life
supervisor lieutenant andrew zabavsky is
also charged accused of attempting to
cover up the seriousness of the chase
chief robert contee addressed the police
force tonight
"""

TRANSCRIPT_HYLTON_B = """
his girlfriend amala jones-bey was at
home with their three month old daughter
when the chase ended his life karon's
mother led protesters at fourth district
amala jones-bey spoke at the vigil
"""


PACKET_FRAZIER = {
    "packet_id": "pcs-test-017",
    "subject_or_case": "Leneal Frazier — Minneapolis PD pursuit fatality (bystander), 2021-07-06",
    "agency": "Minneapolis Police Department",
    "source_urls": [
        "https://kstp.com/local-news/charges-dismissed-against-man-allegedly-involved/",
        "https://www.youtube.com/watch?v=Oy9tlB-UBNg",
    ],
}

TRANSCRIPT_FRAZIER = """
officer brian cummings was pursuing a
carjacking suspect at over 90 miles per
hour through north minneapolis when he
ran a red light and t-boned leneal
frazier's vehicle frazier 40 was killed
his sister cheryl frazier appeared at
the courthouse holding the urn his
cousin terry frazier said this ain't
the freeway cummings was sentenced to
9 months in the workhouse after pleaded
guilty to criminal vehicular homicide
the family hired family attorney ben
crump to represent them in their civil
rights lawsuit against the city cheryl
frazier wept on the stand and terry
frazier later spoke to reporters
"""


# ---------------------------------------------------------------------------
# extract_involved_officers
# ---------------------------------------------------------------------------


def test_officers_extracts_role_token_plus_two_word_name():
    out = extract_involved_officers([TRANSCRIPT_HYLTON_A], PACKET_HYLTON)
    names = [o["name"] for o in out]
    assert "Officer Terrence Sutton" in names
    assert "Lt. Andrew Zabavsky" in names
    assert "Chief Robert Contee" in names


def test_officers_dedupes_across_transcripts():
    """Same officer mentioned in 2 transcripts produces 1 entry."""
    text_a = "officer terrence sutton was charged with murder"
    text_b = "officer terrence sutton was on home detention"
    out = extract_involved_officers([text_a, text_b], PACKET_HYLTON)
    assert len(out) == 1
    assert out[0]["name"] == "Officer Terrence Sutton"


def test_officers_picks_canonical_role_by_frequency():
    """When the same name appears with two roles, pick the more frequent."""
    text = (
        "officer brian cummings was pursuing the suspect "
        "officer brian cummings ran a red light "
        "lieutenant brian cummings was hospitalized"
    )
    out = extract_involved_officers([text], PACKET_HYLTON)
    assert len(out) == 1
    assert out[0]["role"] == "officer"  # 2 hits vs 1


def test_officers_rejects_role_plus_stopwords():
    """A role token followed by filler/stopwords must NOT be captured."""
    # Use words that are >=4 chars but in the stopword list to exercise
    # the long_enough + stopword interaction.
    text = (
        "officer involved heard officer involved said "
        "officer told another officer named earlier"
    )
    out = extract_involved_officers([text], PACKET_HYLTON)
    assert out == []


def test_officers_rejects_single_word_after_role():
    """We require 2+ words after the role token to keep precision high.

    'officer smith' alone is too noisy in caption text — not captured.
    """
    text = "officer smith arrived first then officer jones followed"
    out = extract_involved_officers([text], PACKET_HYLTON)
    assert out == []


def test_officers_fills_agency_from_packet():
    out = extract_involved_officers([TRANSCRIPT_HYLTON_A], PACKET_HYLTON)
    for o in out:
        assert o["agency"] == PACKET_HYLTON["agency"]


def test_officers_status_and_badge_always_none():
    """Per-officer status / badge are not extracted deterministically."""
    out = extract_involved_officers([TRANSCRIPT_HYLTON_A], PACKET_HYLTON)
    for o in out:
        assert o["status"] is None
        assert o["badge"] is None


def test_officers_handles_empty_transcripts():
    assert extract_involved_officers([], PACKET_HYLTON) == []
    assert extract_involved_officers([""], PACKET_HYLTON) == []


# ---------------------------------------------------------------------------
# extract_family_decedent
# ---------------------------------------------------------------------------


def test_family_extracts_relation_then_name_pattern():
    out = extract_family_decedent([TRANSCRIPT_HYLTON_B], PACKET_HYLTON)
    rels = [(r["name"], r["relationship"]) for r in out["primary_relations"]]
    assert ("Amala Jones-Bey", "girlfriend") in rels


def test_family_extracts_name_then_relation_pattern():
    out = extract_family_decedent([TRANSCRIPT_FRAZIER], PACKET_FRAZIER)
    rels = [(r["name"], r["relationship"]) for r in out["primary_relations"]]
    assert ("Cheryl Frazier", "sister") in rels
    assert ("Terry Frazier", "cousin") in rels


def test_family_extracts_family_attorney_pattern():
    out = extract_family_decedent([TRANSCRIPT_FRAZIER], PACKET_FRAZIER)
    rels = [(r["name"], r["relationship"]) for r in out["primary_relations"]]
    assert ("Ben Crump", "family attorney") in rels


def test_family_requires_subject_anchor_in_transcript():
    """A transcript that does not mention the subject's first or last
    name must not produce family relations — guards against firing on
    unrelated transcripts cached under the wrong directory."""
    text = "his sister cheryl smith appeared at the funeral"
    pkt = {
        "subject_or_case": "Karon Hylton-Brown — DC MPD pursuit",
        "agency": "MPDC",
    }
    out = extract_family_decedent([text], pkt)
    assert out == {}


def test_family_rejects_subject_self_as_relation():
    """If the regex captures the subject's own name as a 'relation', drop it."""
    text = (
        "karon hylton-brown was pursued by mpd "
        "karon hylton-brown, his brother, was at the protest"
    )
    out = extract_family_decedent([text], PACKET_HYLTON)
    primary = out.get("primary_relations") or []
    names = [r["name"].lower() for r in primary]
    # Subject's last name must not appear as a relation entry.
    assert not any("hylton" in n for n in names)


def test_family_returns_empty_when_subject_unparseable():
    out = extract_family_decedent([TRANSCRIPT_HYLTON_B], {"subject_or_case": ""})
    assert out == {}


def test_family_emotional_anchors_never_populated_deterministically():
    out = extract_family_decedent([TRANSCRIPT_FRAZIER], PACKET_FRAZIER)
    # The deterministic extractor leaves anchors as a writing/judgment task.
    assert out["emotional_anchors"] == []


def test_family_deduplicates_across_transcripts():
    text_a = "his sister cheryl frazier was there"
    text_b = "his sister cheryl frazier said something"
    out = extract_family_decedent([text_a, text_b], PACKET_FRAZIER)
    rels = [r["name"] for r in out["primary_relations"]]
    assert rels.count("Cheryl Frazier") == 1


# ---------------------------------------------------------------------------
# extract_case_outcome_from_urls
# ---------------------------------------------------------------------------


def test_outcome_url_convicted_slug():
    out = extract_case_outcome_from_urls(PACKET_HYLTON)
    assert out["doj_url"].startswith("https://www.justice.gov/")
    assert "convicted" in out["conviction_status"]
    # Court inference: USAO-DC is the only one we expand to a full district name.
    assert out["court"] == "U.S. Attorney's Office for the District of Columbia"


def test_outcome_url_other_usao_keeps_slug_code():
    """Non-DC USAO codes retain the slug code rather than inventing a
    district name. The slug-to-full-name mapping for the ~94 USAOs is
    not deterministic from the URL alone."""
    pkt = {"source_urls": ["https://www.justice.gov/usao-edny/pr/officer-indicted"]}
    out = extract_case_outcome_from_urls(pkt)
    assert "USAO EDNY" in out["court"]


def test_outcome_url_pleaded_guilty_slug():
    pkt = {"source_urls": [
        "https://www.justice.gov/usao-mn/pr/officer-pleaded-guilty-to-vehicular-homicide",
    ]}
    out = extract_case_outcome_from_urls(pkt)
    assert "pleaded guilty" in out["conviction_status"]


def test_outcome_url_indicted_slug():
    pkt = {"source_urls": ["https://www.justice.gov/usao-ca/pr/officers-indicted"]}
    out = extract_case_outcome_from_urls(pkt)
    assert "indicted" in out["conviction_status"]


def test_outcome_civil_suit_cue_from_news_url():
    pkt = {
        "source_urls": [
            "https://oaklandside.org/2023/01/26/family-soakai-killed-oakland-police-ghost-chase-file-lawsuit/",
        ],
    }
    out = extract_case_outcome_from_urls(pkt)
    assert "lawsuit" in out["civil_suit_status"]


def test_outcome_no_doj_url_returns_no_conviction_keys():
    pkt = {"source_urls": ["https://kstp.com/local-news/some-coverage"]}
    out = extract_case_outcome_from_urls(pkt)
    assert "doj_url" not in out
    assert "conviction_status" not in out


def test_outcome_handles_empty_packet():
    assert extract_case_outcome_from_urls({}) == {}
    assert extract_case_outcome_from_urls({"source_urls": []}) == {}


# ---------------------------------------------------------------------------
# augment_case_outcome_from_transcripts
# ---------------------------------------------------------------------------


def test_augment_does_not_overwrite_url_derived_fields():
    base = {"conviction_status": "convicted (per DOJ URL slug)"}
    text = "officer was sentenced to 9 months and the family filed a wrongful death lawsuit"
    out = augment_case_outcome_from_transcripts(base, [text])
    # URL-derived conviction_status survives.
    assert out["conviction_status"] == "convicted (per DOJ URL slug)"
    # Sentence + civil_suit are added because URL didn't carry them.
    assert "9 months" in out["sentence"]
    assert "civil_suit_status" in out


def test_augment_extracts_sentence_duration_from_transcript():
    text = "cummings was sentenced to 9 months in the workhouse after his guilty plea"
    out = augment_case_outcome_from_transcripts({}, [text])
    assert "9 months" in out["sentence"]


def test_augment_extracts_civil_suit_from_transcript():
    text = "the family filed a federal civil rights lawsuit against the city"
    out = augment_case_outcome_from_transcripts({}, [text])
    assert "civil_suit_status" in out


def test_augment_returns_copy_not_mutation():
    base = {"doj_url": "x"}
    out = augment_case_outcome_from_transcripts(base, [])
    out["new_key"] = "y"
    assert "new_key" not in base


# ---------------------------------------------------------------------------
# enrich_packet
# ---------------------------------------------------------------------------


def test_enrich_packet_populates_all_three_fields():
    enriched, prov = enrich_packet(
        PACKET_HYLTON, [TRANSCRIPT_HYLTON_A, TRANSCRIPT_HYLTON_B]
    )
    assert "involved_officers" in enriched
    assert "family_decedent" in enriched
    assert "case_outcome" in enriched
    assert prov["fields"]["involved_officers"]["count"] >= 3
    assert prov["fields"]["family_decedent"]["count"] >= 1
    assert "doj_url" in prov["fields"]["case_outcome"]["fields_set"]


def test_enrich_packet_does_not_mutate_input():
    """The input packet dict must be unchanged after enrichment."""
    snapshot = dict(PACKET_HYLTON)
    enrich_packet(PACKET_HYLTON, [TRANSCRIPT_HYLTON_A])
    assert PACKET_HYLTON == snapshot
    assert "involved_officers" not in PACKET_HYLTON
    assert "family_decedent" not in PACKET_HYLTON
    assert "case_outcome" not in PACKET_HYLTON


def test_enrich_packet_preserves_existing_optional_fields():
    """When the packet already carries officers/family/outcome, the
    extractor MUST NOT overwrite them — caller is the source of truth."""
    pkt = dict(PACKET_HYLTON)
    pkt["involved_officers"] = [{"name": "Pre-existing Officer", "role": "captain"}]
    pkt["family_decedent"] = {"primary_relations": [{"name": "Pre-existing Sister", "relationship": "sister"}]}
    pkt["case_outcome"] = {"conviction_status": "(pre-existing)"}
    enriched, prov = enrich_packet(pkt, [TRANSCRIPT_HYLTON_A, TRANSCRIPT_HYLTON_B])
    assert enriched["involved_officers"][0]["name"] == "Pre-existing Officer"
    assert enriched["family_decedent"]["primary_relations"][0]["name"] == "Pre-existing Sister"
    assert enriched["case_outcome"]["conviction_status"] == "(pre-existing)"
    # Provenance should not claim to have populated those fields.
    assert "involved_officers" not in prov["fields"]
    assert "family_decedent" not in prov["fields"]
    assert "case_outcome" not in prov["fields"]


def test_enrich_packet_handles_missing_transcripts_gracefully():
    enriched, prov = enrich_packet(PACKET_HYLTON, [])
    # No officers/family extracted, but URL-based outcome still fires.
    assert "involved_officers" not in enriched
    assert "family_decedent" not in enriched
    assert enriched.get("case_outcome", {}).get("doj_url", "").startswith("https://")
    assert prov["transcripts_loaded"] == 0


def test_enrich_packet_provenance_carries_packet_id():
    _, prov = enrich_packet(PACKET_HYLTON, [TRANSCRIPT_HYLTON_A])
    assert prov["packet_id"] == "pcs-test-022"


def test_enrich_packet_accepts_path_objects(tmp_path):
    """Caller can pass file paths instead of pre-read strings."""
    p = tmp_path / "transcript.txt"
    p.write_text(TRANSCRIPT_HYLTON_A, encoding="utf-8")
    enriched, prov = enrich_packet(PACKET_HYLTON, [p])
    assert "involved_officers" in enriched
    assert prov["transcripts_loaded"] == 1
