import json
from pathlib import Path

from pipeline2_discovery.osint_username_harness import (
    build_report,
    extract_entities,
    extract_seeds,
    username_links,
)


def test_extract_explicit_handles_and_email_prefixes():
    text = """
    Defendant Name: Jordan Avery
    Instagram: @javery562
    Reddit user: u/JAVthrowaway
    Email: javery562@gmail.com
    Officer Maria Lopez authored the report.
    """
    entities = extract_entities(text, {})
    seeds = extract_seeds(text, entities)
    by_value = {}
    for seed in seeds:
        by_value.setdefault(seed.value.lower(), []).append(seed)

    assert "javery562" in by_value
    assert any(s.seed_type.startswith("explicit") for s in by_value["javery562"])
    assert "javthrowaway" in by_value
    assert max(s.confidence for s in by_value["javthrowaway"]) >= 90
    assert any(e.name == "Jordan Avery" for e in entities)
    assert any(s.seed_type == "name_variant" and s.entity == "Jordan Avery" for s in seeds)


def test_username_links_match_inteltechniques_style():
    links = username_links("case.user")

    assert links["inteltechniques_username_tool"] == "https://inteltechniques.com/tools/Username.html"
    assert links["instagram"].endswith("/case.user/")
    assert links["reddit_user"].endswith("/user/case.user")
    assert "%22case.user%22" in links["google_exact"]


def test_report_emits_guardrails_for_minor_or_witness():
    text = """
    Witness Name: Alex Rivera
    The report references a juvenile witness and DOB: 01/02/2010.
    Email alex.rivera@example.com appears in an exhibit.
    """
    report = build_report(text=text, doc_extract={}, bundle={}, limit=10)

    assert report["username_candidates"]
    assert any("minor" in f.lower() or "juvenile" in f.lower() for f in report["safety_flags"])
    assert any(e["role"] == "witness" for e in report["entities"])
    assert not any(c["seed_type"] == "name_variant" and c["entity"] == "Alex Rivera" for c in report["username_candidates"])


def test_cli_style_outputs_are_serializable(tmp_path: Path):
    report = build_report(
        text="Subject: Casey Morgan\nIG: casey.morgan\nOfficer Pat Green",
        doc_extract={"subjects": ["Casey Morgan"]},
        bundle={"source": "demo", "case_id": "demo_1"},
        limit=20,
    )
    out = tmp_path / "report.json"
    out.write_text(json.dumps(report), encoding="utf-8")

    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["case_context"]["case_id"] == "demo_1"
    assert any(c["username"] == "casey.morgan" for c in loaded["username_candidates"])
