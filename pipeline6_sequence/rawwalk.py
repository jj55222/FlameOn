"""rawwalk.py — Midwest-Safety-style raw-walk assembler (generalized from the logan cut).

Given a case whose bodycam is AXON-clock stamped (timeline_stamp) and transcribed,
this AUTO-DETECTS the primary involved-officer camera, finds the action peak
(shots fired / use-of-force), and builds a CHRONOLOGICAL sequence of a few LONG
continuous runs — stop/contact -> escalation+action -> aftermath — on that one
camera. Narrated (top-third) + captioned (added at render), NO interviews.

Emits a blueprint consumed by:
    render_blueprint.py --blueprint <out>/<cid>_rawwalk.json --media-dir <basket>/video/Video \
        --transcripts <basket>/d2/transcripts --no-audio-aware --out <cuts>

    python pipeline6_sequence/rawwalk.py --basket .tmp/<cid> --case-id <cid> --agency "..." \
        --out .tmp/<cid>/blueprint_rawwalk [--narrate]
"""
import argparse, glob, json, os, re, sys
from pathlib import Path

# Cue lexicons (domain-general across OIS / UoF / pursuit)
CONTACT = re.compile(r"pulled over|brake light|good evening|license|registration|step out|"
                     r"reason (we|i|you).{0,14}(stop|pull)|do you have|any weapons|roll (down|your) window|"
                     r"driver'?s? license|where (are|you) (going|headed)|stop the car", re.I)
# The action ONSET: the verbalized moment force begins. Prefer "shots fired" and
# force commands; do NOT use bare "shot"/"shooting" — those recur in the medical
# aftermath ("how many times he's shot") and mis-place the peak. When the shooting
# isn't spoken (just commands + gunfire), the "drop the knife/gun" cluster IS the onset.
PEAK = re.compile(r"shots? fired|drop (the |it|that )?(knife|gun|weapon)|drop it\b|"
                  r"put (the |it )?(knife|gun|weapon)?\s*down|\btaser\b|tase (him|her)|"
                  r"stop resisting|hands behind your back now", re.I)
ACTION = re.compile(r"shots? fired|\bshot\b|\bgun\b|\bfirearm\b|drop (it|the)|taser|tase|"
                    r"stop resisting|get on the ground|put your hands|show me your hands|"
                    r"don'?t move|he'?s got|stop fighting|let me see your hands", re.I)
COMMAND = re.compile(r"hands|step out|get out|drop|stop|don'?t move|on the ground|turn (the|your)|"
                     r"show me|put your|get back", re.I)
# Control/medical cues that mark where the action ENDS (used to bound the climax teaser).
AFTER = re.compile(r"turn around|hands behind|roll over|you'?re okay|we'?re okay|keep breathing|"
                   r"\bbreathing\b|\bpulse\b|\bmedic\b|\bcuff|suspect is down|get behind cover|hands out", re.I)


def stem(s): return os.path.splitext(os.path.basename(str(s)))[0]


def load(basket):
    arts = json.load(open(f"{basket}/timeline/artifacts.json"))
    epoch, dur = {}, {}
    for a in arts:
        k = stem(a.get("artifact_id", ""))
        if a.get("kind") == "bodycam" and a.get("start_epoch"):
            epoch[k] = a["start_epoch"]
        dur[k] = a.get("duration_sec", 0) or 0
    tx = {}
    for f in glob.glob(f"{basket}/d2/transcripts/*.json"):
        tx[stem(f)] = json.load(open(f)).get("transcript", [])
    return epoch, dur, tx, arts


def pick_primary(epoch, tx):
    """Primary = the EARLIEST-on-scene bodycam that has real incident presence
    (contact dialogue and/or action cues). The first officer on scene is the
    contact officer who carries the stop -> action -> aftermath arc on one camera;
    a later-arriving supervisor who only RELAYS 'shots fired' on the radio must not
    win on cue count. Tiebreak: more contact+action cues."""
    cand = []
    for k, ep in epoch.items():
        txt = " ".join(s.get("text", "") for s in tx.get(k, []))
        ch, ah = len(CONTACT.findall(txt)), len(ACTION.findall(txt))
        if ch or ah:
            cand.append((ep, -(ch * 4 + ah), k))     # earliest start first
    cand.sort()
    return [k for _, _, k in cand] or list(epoch)


def first_time(segs, ep, rx):
    for s in segs:
        if s.get("text", "").strip() and rx.search(s["text"]):
            return ep + (s.get("start_sec") or 0)
    return None


def windows(pstart, pend, contact, peak):
    """Continuous runs (in_sec) around the action ONSET. POST is wide enough that the
    shooting is captured even when the onset cue (e.g. 'drop the knife') precedes the
    gunfire by ~20s. The stop window collapses gracefully on fast incidents -> 2 runs."""
    PRE, POST, AFT = 115, 45, 55
    c, p = contact - pstart, peak - pstart           # to in_sec
    dur = pend - pstart
    raw = [
        ("stop", max(0.0, c - 5), min(c + 120, p - PRE)),
        ("escalation", max(0.0, p - PRE), p + POST),
        ("aftermath", p + POST, min(dur, p + POST + AFT)),
    ]
    out, last_end = [], -1.0
    for ph, a, z in raw:
        a = max(a, last_end)
        if z - a >= 18:                              # keep only real runs
            out.append((ph, round(a, 1), round(z, 1)))
            last_end = z
    return out


def narrate(basket, phase_text, agency, facts=""):
    """One-line narration per phase, grounded in the transcript AND the documented
    OUTCOME (facts). Critical: gunfire/force is often NOT in the transcript words, so
    transcript-only narration can read a command->'there you go'->handcuff sequence as
    COMPLIANCE when the subject was actually shot. The outcome fact prevents that."""
    generic = {"stop": "Officers make contact.", "escalation": "The situation escalates.",
               "aftermath": "The aftermath."}
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline4_scoring"))
        from llm_backends import build_backend
        sysmsg = ("You narrate police bodycam mini-docs (Midwest-Safety style): ONE plain, factual "
                  "sentence per phase. Ground in the transcript AND the documented outcome. "
                  "CRITICAL: if a shooting or use of force occurred, the narration MUST state it plainly "
                  "(gunfire is often NOT in the transcript words, but it happened) — NEVER imply the person "
                  "complied or it ended peacefully when they were shot or seriously injured. Return JSON.")
        user = (f"DOCUMENTED OUTCOME (ground truth — the narration must be consistent with this): {facts[:600]}\n\n"
                "Write one factual sentence per phase. Return "
                '{"stop":"...","escalation":"...","aftermath":"..."}.\n\n' +
                "\n\n".join(f"[{ph}]\n{txt[:1300]}" for ph, txt in phase_text.items()))
        raw = build_backend("google/gemini-3.1-flash-lite-preview").complete(system=sysmsg, user=user, max_tokens=400, temperature=0.2)
        d = json.loads(raw)
        out = {k: (d.get(k) or generic[k]) for k in generic}
        # accuracy guard: a shooting/death outcome must not be narrated as compliance
        fl = facts.lower()
        if any(w in fl for w in ("shot", "shoot", "fatal", "killed", "kill", "deadly")):
            blob = " ".join(out.values()).lower()
            if any(w in blob for w in ("complied", "cooperat", "peaceful", "without incident")) or \
               not any(w in blob for w in ("shot", "shoot", "fire", "fired", "force")):
                print("[rawwalk] WARN: shooting/death outcome but narration omits/contradicts it — flagging", file=sys.stderr)
                out["_warn"] = "narration may understate the shooting; review"
        return out
    except Exception as e:
        print(f"[rawwalk] narration LLM unavailable ({str(e)[:80]}); generic", file=sys.stderr)
        return generic


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--basket", required=True)
    ap.add_argument("--case-id", required=True)
    ap.add_argument("--agency", default="Releasing agency")
    ap.add_argument("--out", required=True)
    ap.add_argument("--narrate", action="store_true", help="LLM phase narration (else generic)")
    args = ap.parse_args()

    epoch, dur, tx, arts = load(args.basket)
    if not epoch:
        sys.exit("[rawwalk] no AXON-stamped bodycam — run timeline_stamp first")
    primary = pick_primary(epoch, tx)[0]
    psegs, pep = tx.get(primary, []), epoch[primary]
    pdur = dur.get(primary) or (psegs[-1].get("end_sec", 600) if psegs else 600)
    pend = pep + pdur
    peak = first_time(psegs, pep, PEAK) or first_time(psegs, pep, ACTION) or (pep + pdur * 0.7)
    contact = first_time(psegs, pep, CONTACT) or (pep + 20)
    W = windows(pep, pend, contact, peak)
    if not W:
        sys.exit("[rawwalk] could not derive windows")

    # phase transcript text (for narration) from the primary within each window
    def text_in(a, z):
        return " ".join(s.get("text", "") for s in psegs if z >= (s.get("start_sec") or 0) >= a)
    phase_text = {ph: text_in(a, z) for ph, a, z in W}
    # documented outcome for narration grounding: doc_extract -> tier1 verdict -> none
    facts = ""
    de0 = glob.glob(f"{args.basket}/d2/doc_extract*.json")
    if de0:
        dd = json.load(open(de0[0]))
        disp = dd.get("disposition"); disp = disp.get("summary") if isinstance(disp, dict) else disp
        facts = f"{dd.get('subject','')} {(dd.get('narrative') or {}).get('text','')[:300]} {disp or ''}".strip()
    t1 = glob.glob(f"{args.basket}/d2/tier1_verdict.json")
    if not facts and t1:
        v = json.load(open(t1[0]))
        facts = f"{v.get('pitch','')} {v.get('rationale','')}".strip()
    narr = narrate(args.basket, phase_text, args.agency, facts) if args.narrate else \
        {"stop": "Officers make contact.", "escalation": "The situation escalates.", "aftermath": "The aftermath."}

    media_path = next((a["path"] for a in arts if stem(a.get("artifact_id", "")) == primary), None)
    manifest = [{"asset_id": primary, "pov_label": primary, "path": media_path,
                 "kind": "bodycam", "duration_sec": pdur}]
    beats = []
    for i, (ph, a, z) in enumerate(W):
        beats.append({
            "beat_id": f"raw_{i}", "act_id": None, "is_broll": False, "is_document": False,
            "primary_asset": {"asset_id": primary, "in_sec": a, "out_sec": z},
            "narration_bridge": {"text": narr.get(ph, "")} if ph != "aftermath" or narr.get(ph) else {},
            "lower_third": {"text": "Body-Worn Camera"}, "quote": {}, "inserts": [], "source_refs": [],
        })

    # outcome facts from doc_extract if present
    incident = {}
    de = glob.glob(f"{args.basket}/d2/doc_extract*.json")
    if de:
        d = json.load(open(de[0]))
        disp = d.get("disposition")
        if isinstance(disp, dict):                       # render wants a string
            disp = "; ".join(disp.get("discipline_signals") or []) or (disp.get("summary") or "")[:120]
        charges = d.get("charges") or []
        charges = [c for c in charges if isinstance(c, str)]
        incident = {"subject": str(d.get("subject") or ""), "charges": charges,
                    "disposition": str(disp or "")}

    bp = {"case_id": args.case_id, "agency": args.agency,
          "logline": f"{args.agency} bodycam — raw walk.", "acts": [],
          "asset_manifest": manifest, "incident": incident, "beats": beats,
          "metadata": {"built_by": "rawwalk", "primary_camera": primary}}
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    bp_path = out / f"{args.case_id}_rawwalk.json"
    json.dump(bp, open(bp_path, "w"), indent=1)

    import datetime as dt
    def hms(e): return dt.datetime.utcfromtimestamp(e).strftime("%H:%M:%S")
    print(f"[rawwalk] primary camera: {primary}")
    print(f"[rawwalk] contact={hms(contact)}  action_peak={hms(peak)}")
    for ph, a, z in W:
        print(f"   {ph:11} +{a:.0f}s..+{z:.0f}s  ({z-a:.0f}s)  [{hms(pep+a)}-{hms(pep+z)}]")
    print(f"[rawwalk] -> {bp_path}")


if __name__ == "__main__":
    main()
