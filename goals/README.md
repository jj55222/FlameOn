# goals/ — deterministic done-checkers for the week plan

House pattern (AutoResearch discipline): each workstream in
[docs/plans/WEEK_2026-07-01_goals.md](../docs/plans/WEEK_2026-07-01_goals.md) has ONE goal script
here — a deterministic checker that exits **0 = done, 1 = not yet**, printing WHY on failure.
Executors (Opus/Codex) build the checker FIRST per the spec in the plan, then iterate the actual
work (optionally under `/loop`) until it goes green. Checkers are read-only: they never fix, only
verify. Don't loosen a checker to make it pass — that inverts the pattern.

| Script | Workstream | Spec |
|--------|-----------|------|
| `ws1_registry_check.py` | Source sweep → aggregator | plan §WS1 (diff vs `ws1_baseline.json`) |
| `ws2_p0_health.py` | P0 autonomous daily | plan §WS2 |
| `ws3_shortlist_check.py` | EWU evidence-completeness shortlist | plan §WS3 |
| `ws4_remotion_smoke.py` | Remotion render lane | plan §WS4 |

`ws1_baseline.json` — frozen 2026-07-01 registry snapshot (2,769 bundles / 495 media / 5 sources).
Do not regenerate it mid-week; it is the fixed point WS1 is measured against.
