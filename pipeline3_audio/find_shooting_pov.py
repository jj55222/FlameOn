"""GOAL_D — find the officer-involved-shooting POV, cost-effectively.

The funnel keeps the expensive signal last and bounded:

  FREE  local audio impulse ranking (``zip_triage_full.json``, already computed)
        decides WHERE to look — which POVs were loud and WHEN.
  CHEAP one vision pass per candidate (Gemini 2.5 Flash, ~$0.003/POV) decides
        WHAT it is — a discharge / weapon / person-down vs. a parked car or wall.

Vision is the only paid step and it is bounded THREE ways: a per-candidate frame
cap, a candidate cap, and a hard USD budget that aborts the run before the next
call. Real per-call cost is read back from OpenRouter (``usage.include``) and
logged after every call — spend is observed, never estimated after the fact.

Disk-safe like ``zip_triage``: extract one POV from the 42 GB zip, grab frames at
its loud window, score it, DELETE it — never hold two POVs (or two frame sets) at
once. Resumable: results checkpoint after every candidate, so a budget stop or a
crash never re-pays for work already done.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import vision_scan as vs   # sample_frames, batch, parse_events, merge_events, _SYSTEM, _b64_png
import zip_triage as zt    # free_gb (disk guard)

def _load_env() -> None:
    """Best-effort load of OPENROUTER_API_KEY from the repo .env (cwd or repo root)."""
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    for p in (Path.cwd() / ".env", Path(__file__).resolve().parents[1] / ".env"):
        if p.exists():
            load_dotenv(p)
            return


# Gemini-2.5-Flash list price ($/token) — only a FALLBACK if OpenRouter doesn't
# return the real cost in the usage block.
_RATE_IN, _RATE_OUT = 0.30 / 1e6, 2.50 / 1e6

# How strongly each visible event implies "THIS camera saw the shooting".
# Tuned after the top-30 sweep: generic weapon_drawn is DOWN-weighted (evidence
# handling — a gun on a table, loading a magazine — kept scoring as high as a
# real confrontation), while the field-shooting signature (a firearm pointed at a
# person, an officer down, CPR/medical aid) is UP-weighted.
SHOOTING_WEIGHTS = {
    "firearm_discharge": 1.0, "shots_fired": 1.0,
    "officer_down": 0.9, "medical_aid": 0.8, "firearm_pointed": 0.7,
    "foot_pursuit": 0.4, "weapon_drawn": 0.25, "taser": 0.2,
    "takedown": 0.15, "strike": 0.15, "k9_deployment": 0.15,
    "handcuffing": 0.1, "use_of_force_other": 0.15, "search": 0.05, "scene": 0.0,
}

_USER = (
    "These frames are from ONE police camera at its single loudest moment. Decide "
    "whether this is the OFFICER-INVOLVED SHOOTING or its immediate aftermath IN THE "
    "FIELD. The real event looks like: officers behind cover or advancing outdoors, a "
    "firearm POINTED at a person, someone shot and on the ground, CPR / medical aid, "
    "people running. Flag what is visible: firearm_discharge, shots_fired, "
    "firearm_pointed, officer_down, medical_aid, foot_pursuit, weapon_drawn. "
    "IMPORTANT: a firearm being inspected, cleared, loaded, or lying on a table/desk "
    "(evidence handling indoors) is NOT use of force — return a single 'scene'. An "
    "empty road, parked car, or building is also 'scene'. Report only what is visible; "
    "never invent actions."
)


# ---------------------------------------------------------------------------
# Pure scoring / selection (zero-network, unit-tested)
# ---------------------------------------------------------------------------

def shooting_score(events: List[Dict]) -> float:
    """Confidence-weighted evidence that this POV saw the shooting (pure)."""
    return round(sum(SHOOTING_WEIGHTS.get(e.get("event_type", ""), 0.0)
                     * float(e.get("confidence", 0.5)) for e in events), 3)


def candidate_windows(rec: Dict, pad: float = 12.0, dur_cap: float = 30.0) -> List[List[float]]:
    """Loud window(s) to look at: the densest volley cluster (padded), plus the
    top hot_window if it is somewhere else. Clamped to the recording (pure)."""
    dur = float(rec.get("duration_sec") or 0) or 1e9
    wins: List[List[float]] = []
    sc = rec.get("shot_cluster") or {}
    if sc.get("start") is not None:
        s = max(0.0, float(sc["start"]) - pad)
        wins.append([round(s, 1), round(min(dur, s + dur_cap), 1)])
    for hw in (rec.get("hot_windows") or [])[:1]:
        try:
            s = max(0.0, float(hw[0]) - pad)
            w = [round(s, 1), round(min(dur, s + dur_cap), 1)]
        except (TypeError, ValueError, IndexError):
            continue
        if not wins or abs(w[0] - wins[0][0]) > dur_cap:
            wins.append(w)
    return wins or [[0.0, round(min(dur, dur_cap), 1)]]


def select_candidates(triage: List[Dict], top_n: int,
                      skip: Optional[set] = None) -> List[Dict]:
    """Rank by gunshot-volley priority (cluster density, then transient strength,
    then activity) and take the top N (pure). ``skip`` drops members by basename."""
    skip = skip or set()
    pool = [r for r in triage
            if os.path.basename(r.get("member", "")) not in skip]
    pool.sort(key=lambda r: (-(r.get("shot_cluster") or {}).get("count", 0),
                             -float(r.get("impulse_score") or 0),
                             -float(r.get("activity_score") or 0)))
    return pool[:top_n]


# ---------------------------------------------------------------------------
# Cost meter + vision call (the only paid path)
# ---------------------------------------------------------------------------

class CostMeter:
    """Tracks real USD spend against a hard budget; blocks before overshooting."""
    def __init__(self, budget_usd: float):
        self.budget = float(budget_usd)
        self.spent = 0.0
        self.calls = 0
        self.in_tok = 0
        self.out_tok = 0

    def can_spend(self) -> bool:
        return self.spent < self.budget

    def add(self, cost: float, in_tok: int, out_tok: int) -> None:
        self.spent += float(cost)
        self.in_tok += int(in_tok)
        self.out_tok += int(out_tok)
        self.calls += 1


def _usage_cost(usage) -> Optional[float]:
    """OpenRouter returns the real cost in usage.cost (or model_extra)."""
    v = getattr(usage, "cost", None)
    if v is None:
        me = getattr(usage, "model_extra", None) or {}
        v = me.get("cost")
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def make_vision_call(client, model: str) -> Callable[[List[Tuple[float, str]], str],
                                                     Tuple[str, float, int, int]]:
    """Returns call_fn(frames, user) -> (text, cost_usd, in_tok, out_tok). Asks
    OpenRouter to include the real cost in the usage block; falls back to list price."""
    def call_fn(frames, user):
        content: List[Dict] = [{"type": "text", "text": user}]
        for tc, png in frames:
            content.append({"type": "text", "text": f"[t={tc:.1f}s]"})
            content.append({"type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{vs._b64_png(png)}"}})
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": vs._SYSTEM},
                      {"role": "user", "content": content}],
            temperature=0.1, max_tokens=1200, timeout=240,
            extra_body={"usage": {"include": True}},
            extra_headers={"HTTP-Referer": "https://github.com/jj55222/FlameOn",
                           "X-Title": "FlameOn POV Finder"})
        txt = resp.choices[0].message.content or ""
        u = resp.usage
        in_tok = getattr(u, "prompt_tokens", 0) or 0
        out_tok = getattr(u, "completion_tokens", 0) or 0
        cost = _usage_cost(u)
        if cost is None:
            cost = in_tok * _RATE_IN + out_tok * _RATE_OUT
        return txt, float(cost), in_tok, out_tok
    return call_fn


# ---------------------------------------------------------------------------
# The funnel
# ---------------------------------------------------------------------------

def score_candidate(local_media: str, rec: Dict, call_fn, meter: CostMeter,
                    frames_per_win: int, fps: float, frame_dir: Path) -> Dict:
    """Sample the loud window of one already-extracted POV, run the (bounded) vision
    pass, return its scored record. Frames are deleted after scoring."""
    wins = candidate_windows(rec)
    frames = vs.sample_frames(local_media, wins, fps=fps,
                              out_dir=frame_dir, max_frames=frames_per_win)
    events: List[Dict] = []
    call_cost = 0.0
    for fb in vs.batch(frames, 8):
        if not meter.can_spend():
            break
        txt, cost, it, ot = call_fn(fb, _USER)
        meter.add(cost, it, ot)
        call_cost += cost
        events.extend(vs.parse_events(txt))
    for _, png in frames:
        try:
            os.remove(png)
        except OSError:
            pass
    events = vs.merge_events(events)
    return {
        "member": os.path.basename(rec.get("member", "")),
        "size_mb": rec.get("size_mb"),
        "shot_cluster": rec.get("shot_cluster"),
        "windows": wins,
        "n_frames": len(frames),
        "shooting_score": shooting_score(events),
        "event_types": sorted({e["event_type"] for e in events}),
        "events": events,
        "vision_cost_usd": round(call_cost, 5),
    }


def find_pov(zip_path: str, triage_path: str, out_path: str, *,
             top_n: int = 12, budget_usd: float = 1.00,
             model: str = "google/gemini-2.5-flash",
             frames_per_win: int = 8, fps: float = 0.4,
             min_free_gb: float = 3.0, skip: Optional[set] = None,
             call_fn=None, tmp_dir: str = ".tmp/_pov_find") -> List[Dict]:
    triage = json.loads(Path(triage_path).read_text(encoding="utf-8"))
    cands = select_candidates(triage, top_n, skip)
    meter = CostMeter(budget_usd)

    if call_fn is None:                      # live path (default)
        _load_env()
        from openai import OpenAI
        key = os.environ.get("OPENROUTER_API_KEY")
        if not key:
            raise RuntimeError("OPENROUTER_API_KEY not set (load .env)")
        client = OpenAI(api_key=key, base_url="https://openrouter.ai/api/v1")
        call_fn = make_vision_call(client, model)

    z = zipfile.ZipFile(zip_path)
    by_base = {os.path.basename(i.filename): i.filename for i in z.infolist()}
    tmp = Path(tmp_dir); tmp.mkdir(parents=True, exist_ok=True)
    fdir = Path(".tmp/_pov_frames"); fdir.mkdir(parents=True, exist_ok=True)

    results: List[Dict] = []
    if Path(out_path).exists():
        try:
            results = json.loads(Path(out_path).read_text(encoding="utf-8"))
        except ValueError:
            results = []
    done = {r.get("member") for r in results}

    def flush() -> None:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(json.dumps(results, indent=2), encoding="utf-8")

    for k, rec in enumerate(cands):
        base = os.path.basename(rec.get("member", ""))
        if base in done:
            continue
        if not meter.can_spend():
            print(f"[pov] BUDGET STOP ${meter.spent:.4f}/${budget_usd:.2f} before {base}")
            break
        size_mb = float(rec.get("size_mb") or 0)
        if zt.free_gb(tmp_dir) < max(min_free_gb, size_mb / 1000 + 1):
            print(f"[pov] ABORT: low disk before {base}")
            break
        member = by_base.get(base, rec.get("member"))
        local = tmp / base
        print(f"[pov] [{k+1}/{len(cands)}] {base} ({size_mb:.0f} MB)  "
              f"cluster={rec.get('shot_cluster')}  spent ${meter.spent:.4f}")
        sys.stdout.flush()
        try:
            with z.open(member) as s, open(local, "wb") as d:
                shutil.copyfileobj(s, d, 1 << 20)
            scored = score_candidate(str(local), rec, call_fn, meter,
                                     frames_per_win, fps, fdir)
            results.append(scored)
            done.add(base)
            print(f"        -> score {scored['shooting_score']:.2f}  "
                  f"types={scored['event_types']}  (+${scored['vision_cost_usd']:.4f}, "
                  f"total ${meter.spent:.4f})")
            flush()
        except Exception as e:   # noqa: BLE001 — one bad POV shouldn't kill the run
            print(f"        skip ({type(e).__name__}: {str(e)[:90]})")
        finally:
            if local.exists():
                local.unlink()

    results.sort(key=lambda r: -r.get("shooting_score", 0))
    flush()
    print(f"\n[pov] DONE  calls={meter.calls}  tok in/out={meter.in_tok}/{meter.out_tok}  "
          f"SPENT ${meter.spent:.4f} / budget ${budget_usd:.2f}")
    print("[pov] RANKED:")
    for r in results[:10]:
        print(f"  {r.get('member', '?'):12s} score={r.get('shooting_score', 0):.2f}  "
              f"{r.get('event_types', [])}")
    return results


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="GOAL_D — find the shooting POV (audio-ranked → cheap vision)")
    ap.add_argument("--zip", required=True)
    ap.add_argument("--triage", default=".tmp/ois_289964/zip_triage_full.json",
                    help="free audio ranking from zip_triage --ocr-less full scan")
    ap.add_argument("--out", default=".tmp/ois_289964/pov_vision.json")
    ap.add_argument("--top-n", type=int, default=12)
    ap.add_argument("--budget-usd", type=float, default=1.00)
    ap.add_argument("--model", default="google/gemini-2.5-flash")
    ap.add_argument("--frames", type=int, default=8, help="max frames per candidate")
    ap.add_argument("--fps", type=float, default=0.4)
    ap.add_argument("--skip", nargs="*", default=["236.mp4", "195.mp4", "170.mp4"],
                    help="basenames already analyzed (default: the 3 known non-shooting POVs)")
    args = ap.parse_args(argv)
    find_pov(args.zip, args.triage, args.out, top_n=args.top_n, budget_usd=args.budget_usd,
             model=args.model, frames_per_win=args.frames, fps=args.fps, skip=set(args.skip))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
