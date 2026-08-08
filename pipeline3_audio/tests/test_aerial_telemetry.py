"""Zero-OCR tests for aerial_telemetry pure parsers (HUD signature / address /
timestamp), using the real garbled strings observed on the air-unit HUD."""
from __future__ import annotations

import sys
from pathlib import Path

PARENT = Path(__file__).resolve().parent.parent
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

import aerial_telemetry as at  # noqa: E402


def test_is_aerial_hud_detects_signature():
    assert at.is_aerial_hud(["SEARCHLIGHT", "GEOPOINT", "Nearest Parcels"])
    assert at.is_aerial_hud(["LoS: Good", "TGT", "DFLT"])
    assert not at.is_aerial_hud(["a road", "a parked car", "a tree"])


def test_parse_address_handles_two_word_streets():
    assert at.parse_address("4508 EL CAMINO AVE") == "4508 EL CAMINO AVE"
    assert at.parse_address("2924 montclaire st") == "2924 MONTCLAIRE ST"
    assert at.parse_address("2500 GREENWOOD AVE _") == "2500 GREENWOOD AVE"
    assert at.parse_address("just some text") is None


def test_extract_addresses_dedups_preserving_order():
    toks = ["4500 EL CAMINO AVE", "junk", "4500 EL CAMINO AVE", "2500 GREENWOOD AVE"]
    assert at.extract_addresses(toks) == ["4500 EL CAMINO AVE", "2500 GREENWOOD AVE"]


def test_parse_timestamp_recovers_clean_parts():
    ts = at.parse_timestamp("19:04:06 30-06-2017 UTC-7")
    assert ts["time"] == "19:04:06"
    assert ts["date"] == "30-06-2017"
    assert ts["utc_offset"] == "-7"


def test_parse_timestamp_minute_resolution_and_utc():
    # real HUD strings: minute resolution, OCR puts spaces around the separator
    ts = at.parse_timestamp("304162017 RuTO 1 18 : 20 UTC-7")
    assert ts["time"] == "18:20"
    assert ts["utc_offset"] == "-7"


def test_parse_timestamp_tolerates_ocr_semicolon():
    ts = at.parse_timestamp("RuTO 1 19;25- ITTC")   # OCR read ':' as ';'
    assert ts["time"] == "19:25"


def test_parse_timestamp_rejects_invalid_pair():
    ts = at.parse_timestamp("lat 54:43 lon")
    assert ts["time"] is None                # 54 is not a valid hour


def test_parse_timestamp_garbled_keeps_raw():
    ts = at.parse_timestamp("304062017")
    assert ts["time"] is None and ts["date"] is None
    assert ts["raw"] == "304062017"
