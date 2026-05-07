"""Deterministic packet enricher.

Reads cached caption transcripts and the packet stub itself and populates
three of the optional packet schema fields introduced by PR #49:

  - ``involved_officers[]``  — role-token + name pattern, stopword-filtered,
                               deduped across transcripts, canonical name
                               chosen by frequency.
  - ``family_decedent``      — subject-anchored relation patterns (his/her
                               <relation> <name>; <name>, his/her <relation>;
                               family attorney <name>).
  - ``case_outcome``         — primary path is DOJ press-release URL slug
                               parsing; transcript text augments
                               sentence / civil_suit_status when the slug
                               does not carry them.

Out of scope (the harder fields, deferred to a later branch):

  - ``timeline_events[]``    — date extraction is its own problem.
  - ``narrative_spine``      — needs a writer / LLM.
  - ``production_angles[]``  — judgment call, needs a human.

No LLM. No network. No transcript re-extraction. No mutation of the input
packet dict.

Public API
----------

  extract_involved_officers(transcript_texts, packet) -> list[dict]
  extract_family_decedent(transcript_texts, packet) -> dict
  extract_case_outcome_from_urls(packet) -> dict
  augment_case_outcome_from_transcripts(case_outcome, transcript_texts) -> dict
  enrich_packet(packet, transcript_texts) -> tuple[dict, dict]

``transcript_texts`` is always an iterable of raw caption text strings —
the caller is responsible for reading ``.txt`` files. This keeps the
module unit-testable without filesystem fixtures.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Iterable


# Role tokens, ordered most-specific-first so the alternation prefers
# multi-word roles. The whole regex is case-insensitive.
#
# Singular role tokens only — the plural "officers" is intentionally
# excluded because in caption text it's almost always followed by
# common-noun trailing words ("officers actions", "officers said",
# "officers disproportionately") that look like 2-word names but
# aren't. The singular "officer + Firstname Lastname" pattern is the
# overwhelming majority of real officer mentions.
_OFFICER_ROLE_PATTERN = (
    r"special\s+agent|patrol\s+officer|patrolman|"
    r"officer|"
    r"lieutenant|lt\.?|"
    r"sergeant|sgt\.?|"
    r"detective|det\.?|"
    r"trooper|deputy|"
    r"chief|commander|captain|"
    r"corporal|cpl\.?|agent"
)

# Name token: exactly 2 alphabetic words separated by space/hyphen/apostrophe.
# Single-word "officer Smith" matches are not captured — too noisy in caption
# text. Three-word matches are not captured because the third word is almost
# always a trailing verb ("officer Robert Contee addressed") rather than a
# real third name part — our domain has very few legit 3-word names.
_OFFICER_NAME_PATTERN = (
    r"[a-z][a-z\-']+\s+[a-z][a-z\-']+"
)

_OFFICER_REGEX = re.compile(
    r"\b(?P<role>" + _OFFICER_ROLE_PATTERN + r")\s+(?P<name>" + _OFFICER_NAME_PATTERN + r")\b",
    re.IGNORECASE,
)

# Words that, if every captured name word is one of them, mark the match
# as a false positive. Filler verbs, possessives, role-noise, generic
# people-words. Names like "Terrence Sutton" or "Andrew Zabavsky" survive
# because their tokens are not in this set.
_NAME_STOPWORDS = frozenset(
    """
    is was were has had have said says told asked called took made got went came saw
    heard thought knew felt wanted needed tried started began ended killed shot fired
    shooting shooter then when who whom what where why how the a an and but or so on in
    at of to from with by for as that this those these he she it they we you i my his
    her their our your they're they've they'd they'll
    after before during while until since even also just only very too much many all
    some any every no not never ever always often sometimes police officer officers
    department departments named involved unidentified person people man woman men women
    boy girl father mother son daughter brother sister wife husband family friend friends
    neighbor neighbors witness witnesses another other others one two three four five six
    first second third last next previous today tomorrow yesterday tonight morning
    afternoon evening night here there now later soon early late ago back away around
    across down up over under into onto upon between behind beneath above below beside
    near far away again still yet already already once twice
    deceased decedent victim victims suspect suspects accused alleged former current
    incoming outgoing acting interim former retired
    chief commander captain
    actions action statement statements decision decisions reaction reactions
    behavior conduct history training policy procedure procedures comment comments
    response responses
    both all some none many most least few several enough plenty
    because although though however therefore thus hence moreover furthermore
    with within without between among throughout beyond beneath beside
    certainly definitely possibly probably supposedly reportedly allegedly
    could would should may might must will going gonna
    won't can't didn't don't isn't aren't weren't wasn't haven't hasn't
    hadn't wouldn't couldn't shouldn't ain't mustn't
    even ever every each either neither
    truck food store house school office building company city town country
    state county district court department departments
    """.split()
)


# Subject anchor — used for family extraction so we don't fire patterns
# off transcripts unrelated to the case.
_SUBJECT_SEPARATORS = (" — ", " – ", " - ")


# Family relation tokens.
_FAMILY_RELATIONS = (
    "mother", "father", "sister", "brother",
    "girlfriend", "boyfriend", "wife", "husband",
    "daughter", "son", "child", "children",
    "cousin", "niece", "nephew", "aunt", "uncle",
    "grandmother", "grandfather", "grandparent",
    "stepmother", "stepfather", "stepsister", "stepbrother",
)
_RELATION_GROUP = "(?:" + "|".join(_FAMILY_RELATIONS) + ")"


# Pattern A: "<possessive> <relation> <name>".
# The possessive is captured generically (any word ending in "'s" or
# "s'" — "his", "her", "their", or "<subject>'s") and post-filtered in
# extract_family_decedent so we only keep possessives that name the
# subject ("frazier's cousin terry"), not random third parties
# ("the lawyer's wife").
_REL_THEN_NAME = re.compile(
    r"\b(?P<poss>[a-z]+'?s|his|her|their)\s+"
    r"(?P<rel>" + _RELATION_GROUP + r")\s+"
    r"(?P<name>[a-z][a-z\-']+\s+[a-z][a-z\-']+)\b",
    re.IGNORECASE,
)

# Pattern B: "<name>, his/her/the <relation>"
_NAME_THEN_REL = re.compile(
    r"\b(?P<name>[a-z][a-z\-']+\s+[a-z][a-z\-']+),?\s+"
    r"(?:his|her|their|the)\s+(?P<rel>" + _RELATION_GROUP + r")\b",
    re.IGNORECASE,
)

# Pattern C: family/wrongful-death/civil-rights attorney <name>.
# Whitespace runs between "civil" and "rights" / "wrongful" and "death"
# can include newlines in caption text — use ``\s+`` so multi-newline
# splits don't break the match.
_FAMILY_ATTORNEY = re.compile(
    r"\b(?:family|wrongful\s+death|wrongful-death|civil\s+rights?|civil-rights?)\s+attorney\s+"
    r"(?P<name>[a-z][a-z\-']+\s+[a-z][a-z\-']+)\b",
    re.IGNORECASE,
)


# DOJ URL slug cues — primary deterministic source for case_outcome.
_DOJ_SLUG_CONVICTION_CUES = (
    ("convicted", "convicted (per DOJ URL slug)"),
    ("conviction", "convicted (per DOJ URL slug)"),
    ("pleaded-guilty", "pleaded guilty (per DOJ URL slug)"),
    ("pleads-guilty", "pleaded guilty (per DOJ URL slug)"),
    ("guilty-plea", "pleaded guilty (per DOJ URL slug)"),
    ("indicted", "indicted (per DOJ URL slug)"),
    ("indictment", "indicted (per DOJ URL slug)"),
    ("sentenced", "sentenced (per DOJ URL slug)"),
    ("sentencing", "sentenced (per DOJ URL slug)"),
    ("acquitted", "acquitted (per DOJ URL slug)"),
    ("charges-filed", "charged (per DOJ URL slug)"),
    ("charged-with", "charged (per DOJ URL slug)"),
)


# Transcript outcome cues. We do NOT use these as the primary conviction
# source — too noisy without a per-name correlation. We use them to fill
# civil_suit_status and sentence when the URL slug doesn't carry them.
_CIVIL_SUIT_PATTERN = re.compile(
    r"\b(civil[\s\-]rights?\s+lawsuit|wrongful[\s\-]death(?:\s+lawsuit)?|"
    r"civil\s+suit|family\s+(?:files?|filed)\s+(?:a\s+)?lawsuit|"
    r"federal\s+(?:civil[\s\-]rights\s+)?lawsuit)\b",
    re.IGNORECASE,
)

_SENTENCE_PATTERN = re.compile(
    r"\bsentenced\s+to\s+"
    r"(?P<duration>\d+\s+(?:years?|months?|days?|weeks?))\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_subject_name(packet: dict) -> dict:
    """Extract subject {full, last, first} from packet.subject_or_case.

    ``subject_or_case`` is consistently formatted as
    ``<Name> — <description>`` (em dash, en dash, or hyphen). When the
    field is missing or unparseable we return ``{}`` and the caller
    short-circuits family extraction.
    """
    raw = (packet or {}).get("subject_or_case") or ""
    if not raw:
        return {}
    name_part = raw
    for sep in _SUBJECT_SEPARATORS:
        if sep in name_part:
            name_part = name_part.split(sep, 1)[0]
            break
    name_part = name_part.strip()
    if not name_part:
        return {}
    tokens = name_part.split()
    if not tokens:
        return {}
    return {
        "full": name_part,
        "first": tokens[0],
        "last": tokens[-1],
    }


def _looks_like_verb(token: str) -> bool:
    """Cheap heuristic for caption-text past-tense / continuous verbs.

    Catches the dominant false-positive class — captured "name" words
    like "addressed", "arrived", "sentenced", "pursuing" — without
    needing a verb dictionary. Tuned to be conservative on short -ed
    surnames (e.g. "Reed") by requiring length > 4.
    """
    if not token:
        return False
    if token.endswith("ing") and len(token) >= 5:
        return True
    if token.endswith("ed") and len(token) > 4:
        return True
    return False


def _name_is_real(name: str) -> bool:
    """Return True if every word in ``name`` looks like a real-name word.

    Each word must be ≥ 2 chars, not in the stopword set, and not match
    the verb-like heuristic; at least one word must be ≥ 4 chars.
    Hyphens and apostrophes inside a single "word" don't disqualify it.
    Used by both officer and family extraction to filter regex captures.
    """
    if not name:
        return False
    words = re.split(r"\s+", name.strip())
    if not words:
        return False
    long_enough = False
    for w in words:
        token = w.strip("-'").lower()
        if not token or token in _NAME_STOPWORDS or len(token) < 2:
            return False
        if _looks_like_verb(token):
            return False
        if len(token) >= 4:
            long_enough = True
    return long_enough


def _canonical_role(role_token: str) -> str:
    """Normalize a role-token capture into a canonical short form."""
    t = role_token.lower().strip().rstrip(".")
    canonical = {
        "lt": "lieutenant",
        "sgt": "sergeant",
        "det": "detective",
        "cpl": "corporal",
        "officers": "officer",
        "patrol officer": "patrolman",
        "special agent": "special agent",
    }
    return canonical.get(t, t)


def _title_case_name(name: str) -> str:
    """Title-case a lowercase caption-text name. Preserves hyphens and
    apostrophes (so "hylton-brown" -> "Hylton-Brown")."""
    parts = []
    for w in name.split():
        sub = []
        for s in re.split(r"([\-'])", w):
            if s and s.isalpha():
                sub.append(s[:1].upper() + s[1:].lower())
            else:
                sub.append(s)
        parts.append("".join(sub))
    return " ".join(parts)


def _normalize_name_key(name: str) -> str:
    """Lowercase + collapse whitespace + strip punctuation. Used for
    case-insensitive dedupe across transcripts."""
    s = name.lower().strip()
    s = re.sub(r"[\.,;:]+$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _iter_transcripts(transcript_texts: Iterable) -> list[str]:
    """Coerce the transcript_texts argument to a list of strings.

    Accepts strings, ``Path``-like objects (we read), or already-text
    fragments. Empty / None entries are dropped.
    """
    out = []
    for t in transcript_texts or []:
        if t is None:
            continue
        if isinstance(t, str):
            out.append(t)
            continue
        # Path-like
        try:
            text = t.read_text(encoding="utf-8")
        except (AttributeError, OSError):
            continue
        if text:
            out.append(text)
    return out


# ---------------------------------------------------------------------------
# Public API — officer extraction
# ---------------------------------------------------------------------------


def extract_involved_officers(transcript_texts: Iterable, packet: dict) -> list[dict]:
    """Return a list of officer dicts deduped + canonicalized across all
    supplied transcripts.

    Output shape per entry:
      ``{name, role, status, agency, badge}``

    ``status`` and ``badge`` are always ``None`` — those are not reliably
    extractable from auto-caption text. ``agency`` is filled from
    ``packet.agency`` (the packet field is itself deterministic).
    """
    transcripts = _iter_transcripts(transcript_texts)
    if not transcripts:
        return []

    # name_key -> {"display_counts": Counter[display_name], "roles": Counter[role]}
    seen = defaultdict(lambda: {"display": Counter(), "roles": Counter()})

    for text in transcripts:
        for m in _OFFICER_REGEX.finditer(text):
            raw_name = m.group("name").strip()
            if not _name_is_real(raw_name):
                continue
            key = _normalize_name_key(raw_name)
            display = _title_case_name(raw_name)
            seen[key]["display"][display] += 1
            seen[key]["roles"][_canonical_role(m.group("role"))] += 1

    if not seen:
        return []

    agency = (packet or {}).get("agency")
    officers = []
    for key, data in seen.items():
        display = data["display"].most_common(1)[0][0]
        role = data["roles"].most_common(1)[0][0]
        # Compose the rendered name with the role prefix (e.g.
        # "Officer Terrence Sutton") so the renderer in section 4 reads
        # naturally without having to reconstruct it.
        prefix = {
            "officer": "Officer",
            "lieutenant": "Lt.",
            "sergeant": "Sergeant",
            "detective": "Detective",
            "trooper": "Trooper",
            "deputy": "Deputy",
            "chief": "Chief",
            "commander": "Commander",
            "captain": "Captain",
            "corporal": "Corporal",
            "agent": "Agent",
            "special agent": "Special Agent",
            "patrolman": "Patrolman",
        }.get(role, role.title())
        officers.append({
            "name": f"{prefix} {display}",
            "role": role,
            "status": None,
            "agency": agency,
            "badge": None,
        })
    # Stable order: by name.
    officers.sort(key=lambda o: o["name"])
    return officers


# ---------------------------------------------------------------------------
# Public API — family extraction
# ---------------------------------------------------------------------------


def extract_family_decedent(transcript_texts: Iterable, packet: dict) -> dict:
    """Return ``{primary_relations: [...], emotional_anchors: []}`` or ``{}``
    when no high-confidence relation patterns fire.

    Subject anchor: we require ``subject_or_case`` to parse and either the
    subject's first or last name to appear at least once in the
    transcript text. This gates relation patterns to transcripts that are
    actually about the case at hand.

    ``emotional_anchors`` is intentionally never populated by the
    deterministic extractor — anchors are a writing/judgment task.
    """
    transcripts = _iter_transcripts(transcript_texts)
    if not transcripts:
        return {}
    subject = _parse_subject_name(packet)
    if not subject:
        return {}

    first_lc = subject["first"].lower()
    last_lc = subject["last"].lower()
    last_no_hyphen = last_lc.replace("-", " ")
    # Hyphenated last names contribute their first chunk too — captions
    # sometimes drop the rest ("hilton" for "hylton-brown").
    last_first_chunk = last_lc.split("-", 1)[0]
    allowed_possessives = {"his", "her", "their"}
    for n in (first_lc, last_lc, last_first_chunk):
        if n:
            allowed_possessives.add(n)
            allowed_possessives.add(n + "s")  # plain "frazier's" -> rstripped to "frazier" or "fraziers"

    relations_by_key = defaultdict(lambda: {
        "display": Counter(),
        "rel": Counter(),
    })

    for text in transcripts:
        text_lc = text.lower()
        # Subject anchor: at least one of first/last (or hyphen-stripped
        # last) must appear in this transcript. Otherwise skip.
        if not (first_lc in text_lc or last_lc in text_lc or last_no_hyphen in text_lc):
            continue

        for m in _REL_THEN_NAME.finditer(text):
            poss_raw = m.group("poss").strip().lower()
            poss_norm = poss_raw.rstrip("'").rstrip("s").rstrip("'")
            # Accept "his"/"her"/"their" verbatim; otherwise require the
            # possessive to be the subject's first or last name.
            if poss_raw not in {"his", "her", "their"}:
                if poss_norm not in allowed_possessives and poss_raw not in allowed_possessives:
                    continue
            raw_name = m.group("name").strip()
            rel = m.group("rel").strip().lower()
            if not _name_is_real(raw_name):
                continue
            key = _normalize_name_key(raw_name)
            if first_lc and first_lc in key:
                continue
            relations_by_key[key]["display"][_title_case_name(raw_name)] += 1
            relations_by_key[key]["rel"][rel] += 1

        for m in _NAME_THEN_REL.finditer(text):
            raw_name = m.group("name").strip()
            rel = m.group("rel").strip().lower()
            if not _name_is_real(raw_name):
                continue
            key = _normalize_name_key(raw_name)
            if first_lc and first_lc in key:
                continue
            relations_by_key[key]["display"][_title_case_name(raw_name)] += 1
            relations_by_key[key]["rel"][rel] += 1

        for m in _FAMILY_ATTORNEY.finditer(text):
            raw_name = m.group("name").strip()
            if not _name_is_real(raw_name):
                continue
            key = _normalize_name_key(raw_name)
            relations_by_key[key]["display"][_title_case_name(raw_name)] += 1
            relations_by_key[key]["rel"]["family attorney"] += 1

    if not relations_by_key:
        return {}

    # Recurrence guard: a real family-member name almost always shows up
    # more than once across the cached transcripts (or shows up in a
    # second pattern). One-off matches like "his mother food truck"
    # or "his father won't know" — where a stray noun phrase or
    # contraction gets captured — are filtered here. Family attorneys
    # are exempt because they're commonly named once near a single
    # cue phrase ("family attorney Ben Crump") and their cue phrase
    # is itself high-precision.
    # Whitespace-collapsed for substring counting — caption text breaks
    # names across line boundaries ("cheryl\nfrazier"), and the raw
    # text would undercount real recurring names.
    joined_lc = re.sub(r"\s+", " ", "\n".join(t.lower() for t in transcripts))

    primary = []
    for key, data in relations_by_key.items():
        display = data["display"].most_common(1)[0][0]
        rel = data["rel"].most_common(1)[0][0]
        if rel == "family attorney":
            primary.append({"name": display, "relationship": rel})
            continue
        # Count name occurrences across joined transcripts. A real
        # name typically recurs; a one-off noun-phrase capture does not.
        if joined_lc.count(key) < 2:
            continue
        primary.append({"name": display, "relationship": rel})
    if not primary:
        return {}
    primary.sort(key=lambda r: r["name"])

    return {
        "primary_relations": primary,
        "emotional_anchors": [],
    }


# ---------------------------------------------------------------------------
# Public API — case_outcome extraction
# ---------------------------------------------------------------------------


def extract_case_outcome_from_urls(packet: dict) -> dict:
    """Parse DOJ press-release URL slugs in ``packet.source_urls``.

    Returns a dict with whichever of ``conviction_status / sentence /
    doj_url / civil_suit_status / court`` could be derived from URL
    structure alone. Empty fields are not included.
    """
    urls = list((packet or {}).get("source_urls") or [])
    out = {}

    doj_urls = [u for u in urls if "justice.gov" in u.lower()]
    if doj_urls:
        out["doj_url"] = doj_urls[0]
        slug = doj_urls[0].lower()
        cues = []
        for needle, label in _DOJ_SLUG_CONVICTION_CUES:
            if needle in slug and label not in cues:
                cues.append(label)
        if cues:
            out["conviction_status"] = "; ".join(cues)
        # Court inference from USAO district segment. The USAO slug code
        # is preserved verbatim — we don't try to map every code to a
        # full district name (there are ~94 USAOs and the mapping is
        # not deterministic from the slug alone). For DC we expand
        # because the abbreviation is universally recognized.
        m = re.search(r"/usao-([a-z\-]+)/", doj_urls[0].lower())
        if m:
            code = m.group(1)
            if code == "dc":
                out["court"] = "U.S. Attorney's Office for the District of Columbia"
            else:
                out["court"] = f"U.S. Attorney's Office (USAO {code.upper()})"

    # Civil suit cue from any source URL slug or path.
    for u in urls:
        if re.search(r"lawsuit|civil-rights|wrongful-death|family-files", u.lower()):
            if "civil_suit_status" not in out:
                out["civil_suit_status"] = "lawsuit referenced in source URL"
            break

    return out


def augment_case_outcome_from_transcripts(
    case_outcome: dict, transcript_texts: Iterable
) -> dict:
    """Augment a URL-derived ``case_outcome`` with cues found in
    transcript text. Never overwrites a field the URL path already set —
    the URL is the more deterministic source. Returns a new dict.
    """
    out = dict(case_outcome or {})
    transcripts = _iter_transcripts(transcript_texts)
    if not transcripts:
        return out
    joined = "\n".join(transcripts)

    if "civil_suit_status" not in out:
        m = _CIVIL_SUIT_PATTERN.search(joined)
        if m:
            phrase = re.sub(r"\s+", " ", m.group(1)).strip().lower()
            out["civil_suit_status"] = f"transcript reference to {phrase}"

    if "sentence" not in out:
        m = _SENTENCE_PATTERN.search(joined)
        if m:
            out["sentence"] = (
                f"sentenced to {m.group('duration').strip()} (per transcript)"
            )

    return out


# ---------------------------------------------------------------------------
# Public API — orchestrator
# ---------------------------------------------------------------------------


def enrich_packet(packet: dict, transcript_texts: Iterable) -> tuple[dict, dict]:
    """Run all extractors on a packet + its cached transcripts.

    Never mutates the input packet. Pre-existing optional fields on the
    packet are preserved as-is and the extractor's output is dropped on
    the floor for that field — the caller is the source of truth, the
    extractor is a populator for empty fields only.

    Returns ``(enriched_packet, provenance)`` where ``provenance`` carries
    per-field ``source`` and ``count`` metadata so the CLI can write a
    structured audit alongside the enriched JSONL.
    """
    packet = packet or {}
    enriched = dict(packet)
    provenance = {
        "packet_id": packet.get("packet_id"),
        "transcripts_loaded": 0,
        "fields": {},
    }
    transcripts = _iter_transcripts(transcript_texts)
    provenance["transcripts_loaded"] = len(transcripts)

    # Officers — only populate when the packet carries no officers yet.
    if not packet.get("involved_officers"):
        officers = extract_involved_officers(transcripts, packet)
        if officers:
            enriched["involved_officers"] = officers
            provenance["fields"]["involved_officers"] = {
                "source": "transcript_role_token_pattern",
                "count": len(officers),
            }

    # Family — populate only if packet has no family_decedent.
    if not packet.get("family_decedent"):
        family = extract_family_decedent(transcripts, packet)
        if family.get("primary_relations"):
            enriched["family_decedent"] = family
            provenance["fields"]["family_decedent"] = {
                "source": "transcript_subject_anchored_relation_pattern",
                "count": len(family["primary_relations"]),
            }

    # Case outcome — URL slug primary, transcript augmentation secondary.
    if not packet.get("case_outcome"):
        outcome = extract_case_outcome_from_urls(packet)
        outcome = augment_case_outcome_from_transcripts(outcome, transcripts)
        if outcome:
            enriched["case_outcome"] = outcome
            provenance["fields"]["case_outcome"] = {
                "source": "doj_url_slug_plus_transcript_augmentation",
                "fields_set": sorted(outcome.keys()),
            }

    return enriched, provenance
