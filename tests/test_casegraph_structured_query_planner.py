import json
from pathlib import Path

from pipeline2_discovery.casegraph import parse_wapo_uof_case_input, plan_queries_from_structured_result


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "structured_inputs"
CONNECTORS = {"youtube", "muckrock", "courtlistener", "documentcloud", "future_brave_exa"}


def load_fixture(name):
    with (FIXTURES / name).open("r", encoding="utf-8") as f:
        return json.load(f)


def plan_fixture(name):
    parsed = parse_wapo_uof_case_input(load_fixture(name))
    return parsed, plan_queries_from_structured_result(parsed)


def plan_by_connector(result, connector_name):
    return {plan.connector_name: plan for plan in result.plans}[connector_name]


def all_queries(result):
    return [query.query for plan in result.plans for query in plan.queries]


def test_structured_plan_targets_all_expected_connectors_without_live_calls():
    _, result = plan_fixture("wapo_uof_complete.json")

    assert {plan.connector_name for plan in result.plans} == CONNECTORS
    assert all(plan.live_enabled is False for plan in result.plans)
    assert "structured_dataset_query_plan_only" in result.risk_flags
    assert "candidate_fields_not_identity_lock" in result.risk_flags


def test_youtube_plan_uses_agency_critical_incident_video_year_and_name():
    _, result = plan_fixture("wapo_uof_complete.json")
    youtube = plan_by_connector(result, "youtube")
    queries = "\n".join(query.query for query in youtube.queries)

    assert "Phoenix Police Department" in queries
    assert "critical incident video" in queries
    assert "2022" in queries
    assert "John Example" in queries
    assert "possible_artifact_source" in youtube.queries[0].expected_source_roles
    assert "claim_source" in youtube.queries[0].expected_source_roles


def test_muckrock_plan_targets_name_agency_bodycam_records():
    _, result = plan_fixture("wapo_uof_complete.json")
    muckrock = plan_by_connector(result, "muckrock")
    queries = "\n".join(query.query for query in muckrock.queries)

    assert '"John Example"' in queries
    assert "Phoenix Police Department" in queries
    assert "bodycam records" in queries
    assert any("claim_source_not_artifact_source" in query.risk_flags for query in muckrock.queries)


def test_courtlistener_plan_uses_name_and_state_for_outcome_corroboration():
    _, result = plan_fixture("wapo_uof_complete.json")
    court = plan_by_connector(result, "courtlistener")

    assert court.queries
    assert court.queries[0].query == '"John Example" Arizona'
    assert "outcome_source" in court.queries[0].expected_source_roles
    assert "outcome_verification_required" in court.queries[0].risk_flags


def test_future_plans_include_artifact_terms_but_remain_disabled():
    _, result = plan_fixture("wapo_uof_complete.json")
    doccloud = plan_by_connector(result, "documentcloud")
    broad = plan_by_connector(result, "future_brave_exa")
    query_text = "\n".join(all_queries(result))

    assert "bodycam" in query_text
    assert "critical incident video" in query_text
    assert doccloud.live_enabled is False
    assert broad.live_enabled is False
    assert "future_connector" in doccloud.risk_flags
    assert "requires_explicit_opt_in" in broad.risk_flags


def test_missing_structured_fields_are_preserved_without_invention():
    parsed, result = plan_fixture("wapo_uof_missing_fields.json")
    query_text = "\n".join(all_queries(result))

    assert parsed.case_input.known_fields["defendant_names"] == []
    assert "John Example" not in query_text
    assert "Example County Sheriff's Office" in query_text
    assert "2024" in query_text
    assert "missing_subject_name" in result.risk_flags
    assert "subject_name" in plan_by_connector(result, "courtlistener").missing_field_requirements


def test_structured_plan_does_not_set_final_casegraph_decisions():
    _, result = plan_fixture("wapo_uof_complete.json")
    fields = result.case_input.known_fields

    assert "identity_confidence" not in fields
    assert "verified_artifacts" not in fields
    assert "verdict" not in fields
    assert not hasattr(result, "verified_artifacts")
    assert not hasattr(result, "verdict")
