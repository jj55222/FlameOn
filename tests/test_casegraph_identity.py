from pipeline2_discovery.casegraph import SourceRecord, resolve_identity, route_manual_defendant_jurisdiction


def source(
    text,
    *,
    source_authority="news",
    source_roles=None,
    url="https://example.org/source",
    title="Fixture source",
):
    return SourceRecord(
        source_id="fixture_source",
        url=url,
        title=title,
        snippet=text,
        raw_text=text,
        source_type="news" if source_authority == "news" else source_authority,
        source_roles=source_roles or ["identity_source"],
        source_authority=source_authority,
        api_name=None,
        discovered_via="identity_fixture",
        retrieved_at="2026-04-29T23:45:00Z",
        case_input_id="fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=[],
    )


def test_strong_identity_lock_with_authority_and_case_number():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.case_identity.case_numbers = ["SF-2024-001"]
    packet.sources.append(source(
        "Court records for Min Jian Guan in San Francisco, CA list case number SF-2024-001.",
        source_authority="court",
    ))

    result = resolve_identity(packet)

    assert result.identity_confidence == "high"
    assert result.identity_score >= 80.0
    assert "full_name" in result.identity_anchors
    assert "city" in result.identity_anchors
    assert "state" in result.identity_anchors
    assert {"case_number", "source_authority"} & set(result.identity_anchors)
    assert packet.case_identity.identity_confidence == "high"
    assert packet.scores.identity_score == result.identity_score


def test_weak_last_name_only_match_is_not_high():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(source("Guan was mentioned in a California article."))

    result = resolve_identity(packet)

    assert result.identity_confidence != "high"
    assert {"weak_identity", "insufficient_identity_anchors"} & set(result.risk_flags)


def test_name_city_only_collision_does_not_high_lock():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(source("Min Jian Guan appeared in a San Francisco report."))

    result = resolve_identity(packet)

    assert result.identity_confidence != "high"
    assert {"name_city_only", "missing_disambiguator"} & set(result.risk_flags)
    assert result.missing_disambiguators


def test_artifact_looking_source_does_not_prove_identity_or_create_artifact():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(source(
        "Bodycam footage from a police investigation.",
        source_roles=["artifact_source"],
        source_authority="third_party",
        url="https://www.youtube.com/watch?v=fixture",
        title="Bodycam video",
    ))

    result = resolve_identity(packet)

    assert result.identity_confidence != "high"
    assert "insufficient_identity_anchors" in result.risk_flags
    assert packet.verified_artifacts == []


def test_conflicting_jurisdiction_prevents_high_lock():
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.sources.append(source(
        "Official records identify Min Jian Guan in Miami, Florida with case number FL-2024-001.",
        source_authority="official",
    ))

    result = resolve_identity(packet)

    assert result.identity_confidence != "high"
    assert "conflicting_jurisdiction" in result.risk_flags
