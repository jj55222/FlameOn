import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_youtube_case_input, plan_queries_from_youtube_result


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "youtube_inputs"
CONNECTORS = {"youtube", "muckrock", "courtlistener", "documentcloud", "future_brave_exa"}


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def plan_fixture(name):
    parsed = parse_youtube_case_input(load_fixture(name))
    return parsed, plan_queries_from_youtube_result(parsed)


def plan_by_connector(result, connector_name):
    return {plan.connector_name: plan for plan in result.plans}[connector_name]


def all_queries(result):
    return [query.query for plan in result.plans for query in plan.queries]


def test_plans_all_expected_connectors_without_live_execution_flags():
    _, result = plan_fixture("florida_disturbance.json")

    assert {plan.connector_name for plan in result.plans} == CONNECTORS
    assert all(plan.live_enabled is False for plan in result.plans)
    assert "weak_input_query_plan_only" in result.risk_flags
    assert "candidate_fields_not_identity_lock" in result.risk_flags


def test_partial_florida_terms_are_preserved_without_invented_identity():
    parsed, result = plan_fixture("partial_fields_query_generation.json")
    query_text = "\n".join(all_queries(result))

    assert "April 21, 2024" in query_text
    assert "Florida" in query_text
    assert "disabled vehicle" in query_text
    assert "physical disturbance" in query_text
    assert "John Example" not in query_text
    assert "Phoenix Police Department" not in query_text
    assert parsed.case_input.known_fields["defendant_names"] == []
    assert "defendant_names" in plan_by_connector(result, "courtlistener").missing_field_requirements


def test_transcript_name_and_agency_are_candidate_only_plan_anchors():
    _, result = plan_fixture("transcript_suspect_agency_date.json")
    court_plan = plan_by_connector(result, "courtlistener")
    muckrock_plan = plan_by_connector(result, "muckrock")
    query_text = "\n".join(all_queries(result))

    assert "John Example" in query_text
    assert "Phoenix Police Department" in query_text
    assert "May 12, 2022" in query_text
    assert all("candidate_identity_only" in query.risk_flags for query in court_plan.queries)
    assert any("claim_source_not_artifact_source" in query.risk_flags for query in muckrock_plan.queries)
    assert "identity_confidence" not in result.case_input.known_fields


def test_artifact_language_is_planned_as_claim_or_possible_artifact_only():
    _, result = plan_fixture("transcript_artifact_language.json")
    youtube_plan = plan_by_connector(result, "youtube")
    query_text = "\n".join(all_queries(result))

    assert "bodycam" in query_text
    assert "911 call" in query_text
    assert "interrogation" in query_text
    assert "possible_artifact_source" in youtube_plan.queries[0].expected_source_roles
    assert "claim_source" in youtube_plan.queries[0].expected_source_roles
    assert not hasattr(result, "verified_artifacts")


def test_noisy_clickbait_does_not_invent_case_anchors():
    _, result = plan_fixture("noisy_clickbait.json")
    query_text = "\n".join(all_queries(result))

    assert "bodycam" in query_text
    assert "interrogation" in query_text
    assert "Phoenix" not in query_text
    assert "Florida" not in query_text
    assert "John Example" not in query_text
    assert "no_case_anchors" in result.risk_flags
    assert plan_by_connector(result, "courtlistener").queries == []
    assert plan_by_connector(result, "muckrock").queries == []


def test_every_planned_query_explains_why_it_exists():
    _, result = plan_fixture("transcript_suspect_agency_date.json")

    planned_queries = [query for plan in result.plans for query in plan.queries]
    assert planned_queries
    assert all(query.reason for query in planned_queries)
    assert all(query.candidate_fields_used for query in planned_queries)
    assert all(plan.rationale for plan in result.plans)


def test_query_plan_does_not_set_final_casegraph_decisions():
    _, result = plan_fixture("transcript_suspect_agency_date.json")
    fields = result.case_input.known_fields

    assert "identity_confidence" not in fields
    assert "verified_artifacts" not in fields
    assert "verdict" not in fields
    assert not hasattr(result, "verdict")
    assert not hasattr(result, "scores")
