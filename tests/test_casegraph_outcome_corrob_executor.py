"""OUTCOME3 - mocked outcome corroboration executor tests."""
import json

from pipeline2_discovery.casegraph.outcome_corrob_executor import (
    execute_mock_outcome_corroboration,
    outcome_corroboration_to_jsonable,
)
from pipeline2_discovery.casegraph.outcome_seed_plan import OutcomeSeedPlan


def _plan(**overrides):
    base = {
        "case_id": 4,
        "title": "Christa Gail Pike, Tadaryl Shipp",
        "jurisdiction": "Knoxville, Knox County, Tennessee",
        "state": "Tennessee",
        "agency": None,
        "current_outcome_seed_status": "unknown",
        "missing_outcome_reason": "document_signal_without_outcome_text",
        "recommended_outcome_sources": ["courtlistener", "court_docket_search"],
        "suggested_deterministic_query_seeds": ["Christa Gail Pike Knoxville sentenced"],
        "likely_portal_profile": "courtlistener_search",
        "supported_live_path_available": True,
        "priority": "high",
        "blocker": None,
    }
    base.update(overrides)
    return OutcomeSeedPlan(**base)


def test_executor_extracts_sentenced_from_mock_court_payload():
    result = execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "court_docket",
            "source_authority": "court",
            "text": "Court records in Knoxville, Tennessee show Christa Gail Pike was sentenced to death.",
        },
    )

    assert result.extracted_outcome_status == "sentenced"
    assert result.confidence >= 0.9
    assert "sentenced" in result.supporting_snippet.lower()
    assert result.risk_flags == []


def test_executor_extracts_convicted_from_mock_news_payload():
    result = execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "news",
            "source_authority": "news",
            "text": "Christa Gail Pike was convicted after a Knoxville trial.",
        },
    )

    assert result.extracted_outcome_status == "convicted"
    assert 0.7 <= result.confidence < 0.9


def test_executor_extracts_dismissed_acquitted_and_charged_signals():
    dismissed = execute_mock_outcome_corroboration(
        _plan(title="Jane Example", jurisdiction="Phoenix, Arizona", state="Arizona"),
        {
            "source_type": "court",
            "source_authority": "court",
            "text": "Jane Example Phoenix Arizona case dismissed by judge.",
        },
    )
    acquitted = execute_mock_outcome_corroboration(
        _plan(title="John Example", jurisdiction="Mesa, Arizona", state="Arizona"),
        {
            "source_type": "court",
            "source_authority": "court",
            "text": "John Example Mesa Arizona was found not guilty by a jury.",
        },
    )
    charged = execute_mock_outcome_corroboration(
        _plan(title="Robert Example", jurisdiction="Miami, Florida", state="Florida"),
        {
            "source_type": "news",
            "source_authority": "news",
            "text": "Robert Example of Miami Florida was charged with murder.",
        },
    )

    assert dismissed.extracted_outcome_status == "dismissed"
    assert acquitted.extracted_outcome_status == "acquitted"
    assert charged.extracted_outcome_status == "charged"


def test_executor_keeps_weak_or_ambiguous_payload_unknown():
    result = execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "news",
            "source_authority": "news",
            "text": "The case drew renewed attention, but the article does not state a final outcome.",
        },
    )

    assert result.extracted_outcome_status == "unknown"
    assert result.confidence == 0.0
    assert "outcome_unknown" in result.risk_flags
    assert result.next_actions


def test_executor_flags_missing_identity_anchor_without_hallucinating():
    result = execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "news",
            "source_authority": "news",
            "text": "A different person in Miami was sentenced to 25 years in prison.",
        },
    )

    assert result.extracted_outcome_status == "sentenced"
    assert "identity_anchor_missing" in result.risk_flags
    assert result.confidence < 0.8


def test_executor_result_is_json_serializable():
    result = execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "court_docket",
            "source_authority": "court",
            "text": "Court records in Knoxville, Tennessee show Christa Gail Pike was sentenced to death.",
        },
    )

    encoded = json.dumps(outcome_corroboration_to_jsonable(result), sort_keys=True)
    decoded = json.loads(encoded)

    assert decoded["case_id"] == 4
    assert decoded["extracted_outcome_status"] == "sentenced"


def test_executor_makes_no_network_calls(monkeypatch):
    import requests

    calls = []

    def fake_get(self, *args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("network should not be called")

    monkeypatch.setattr(requests.Session, "get", fake_get)

    execute_mock_outcome_corroboration(
        _plan(),
        {
            "source_type": "court_docket",
            "source_authority": "court",
            "text": "Court records in Knoxville, Tennessee show Christa Gail Pike was sentenced to death.",
        },
    )
    assert calls == []
