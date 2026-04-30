from pipeline2_discovery.casegraph import SourceRecord, resolve_outcome, route_manual_defendant_jurisdiction


def source(
    text,
    *,
    source_authority="news",
    source_roles=None,
    source_id="fixture_source",
):
    return SourceRecord(
        source_id=source_id,
        url=f"https://example.org/{source_id}",
        title="Fixture outcome source",
        snippet=text,
        raw_text=text,
        source_type="news" if source_authority == "news" else source_authority,
        source_roles=source_roles or ["identity_source", "outcome_source"],
        source_authority=source_authority,
        api_name=None,
        discovered_via="outcome_fixture",
        retrieved_at="2026-04-29T23:55:00Z",
        case_input_id="fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=["defendant_full_name", "city"],
    )


def packet_with_identity(confidence="high"):
    packet = route_manual_defendant_jurisdiction("Min Jian Guan", "San Francisco, San Francisco, CA")
    packet.case_identity.identity_confidence = confidence
    packet.case_identity.identity_anchors = ["full_name", "city", "state", "source_authority"]
    return packet


def test_sentenced_from_court_source_high_confidence():
    packet = packet_with_identity("high")
    packet.sources.append(source(
        "Court records show Min Jian Guan in San Francisco was sentenced to 25 years in prison.",
        source_authority="court",
        source_id="court_sentenced",
    ))

    result = resolve_outcome(packet)

    assert result.outcome_status == "sentenced"
    assert result.outcome_confidence == "high"
    assert any(anchor.startswith("sentenced:") for anchor in result.outcome_anchors)
    assert result.supporting_sources == ["court_sentenced"]
    assert packet.case_identity.outcome_status == "sentenced"
    assert packet.scores.outcome_score == result.outcome_score


def test_charged_from_news_source_at_least_medium():
    packet = packet_with_identity("high")
    packet.sources.append(source(
        "Min Jian Guan of San Francisco was charged with murder.",
        source_authority="news",
        source_id="news_charged",
    ))

    result = resolve_outcome(packet)

    assert result.outcome_status == "charged"
    assert result.outcome_confidence in {"medium", "high"}


def test_convicted_from_guilty_plea():
    packet = packet_with_identity("medium")
    packet.sources.append(source(
        "Court filings say Min Jian Guan in San Francisco pleaded guilty.",
        source_authority="court",
        source_id="court_plea",
    ))

    result = resolve_outcome(packet)

    assert result.outcome_status == "convicted"
    assert result.outcome_confidence in {"medium", "high"}
    assert any(anchor.startswith("convicted:") for anchor in result.outcome_anchors)


def test_weak_identity_prevents_high_outcome():
    packet = packet_with_identity("low")
    packet.case_identity.identity_anchors = []
    packet.sources.append(source(
        "Min Jian Guan in San Francisco was sentenced to life in prison.",
        source_authority="news",
        source_id="weak_sentenced",
    ))

    result = resolve_outcome(packet)

    assert result.outcome_status == "sentenced"
    assert result.outcome_confidence != "high"
    assert {"weak_identity", "identity_unconfirmed"} & set(result.risk_flags)


def test_dismissed_and_acquitted_override_charged_when_strong():
    dismissed_packet = packet_with_identity("high")
    dismissed_packet.sources.extend([
        source(
            "Min Jian Guan in San Francisco was charged with assault.",
            source_authority="news",
            source_id="news_charged",
        ),
        source(
            "Court records for Min Jian Guan in San Francisco say the case dismissed by judge.",
            source_authority="court",
            source_id="court_dismissed",
        ),
    ])

    dismissed = resolve_outcome(dismissed_packet)

    assert dismissed.outcome_status == "dismissed"
    assert dismissed.outcome_confidence == "high"
    assert "conflicting_outcome_signals" in dismissed.risk_flags

    acquitted_packet = packet_with_identity("high")
    acquitted_packet.sources.append(source(
        "Official records say Min Jian Guan in San Francisco was found not guilty.",
        source_authority="official",
        source_id="official_acquitted",
    ))

    acquitted = resolve_outcome(acquitted_packet)

    assert acquitted.outcome_status == "acquitted"
    assert acquitted.outcome_confidence == "high"


def test_unrelated_sentenced_text_does_not_high_confidence():
    packet = packet_with_identity("low")
    packet.case_identity.identity_anchors = []
    packet.sources.append(SourceRecord(
        source_id="unrelated_sentenced",
        url="https://example.org/unrelated",
        title="Unrelated sentencing article",
        snippet="A different person in Miami, Florida was sentenced to 10 years in prison.",
        raw_text="A different person in Miami, Florida was sentenced to 10 years in prison.",
        source_type="news",
        source_roles=["outcome_source"],
        source_authority="news",
        api_name=None,
        discovered_via="outcome_fixture",
        retrieved_at="2026-04-29T23:55:00Z",
        case_input_id="fixture",
        metadata={},
        cost_estimate=0.0,
        confidence_signals={},
        matched_case_fields=[],
    ))
    packet.risk_flags.append("conflicting_jurisdiction")

    result = resolve_outcome(packet)

    assert result.outcome_status == "sentenced"
    assert result.outcome_confidence == "low"
    assert {"weak_identity", "conflicting_jurisdiction", "identity_unconfirmed"} & set(result.risk_flags)
