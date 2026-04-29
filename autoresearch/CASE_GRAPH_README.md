# Case Graph Harness Fork

This branch adds a safer research fork that tests the case-graph approach without replacing the current `research.py` loop.

## Files

- `research_case_graph.py` — forked research harness with identity locking, artifact claims, artifact resolvers, and legacy-compatible output.
- `evaluate_actionability.py` — production-oriented evaluator that measures identity lock, claim discovery, verified artifact URLs, downloadable URLs, and false-high confidence on INSUFFICIENT cases.

## Why this fork exists

The existing harness optimizes broad evidence recall from source snippets. That is useful, but it can over-count evidence mentions as evidence availability.

The case graph fork separates:

1. **Identity lock** — is this the right person/case/jurisdiction?
2. **Artifact claim** — does a source claim bodycam/interrogation/court/911/docket material exists?
3. **Artifact resolution** — did we find a concrete public artifact URL?
4. **Actionability** — is the artifact official, court/FOIA-hosted, or downloadable/transcript-ready?

## Smoke test

```bash
cd autoresearch
python research_case_graph.py \
  --name "Min Jian Guan" \
  --jurisdiction "San Francisco, San Francisco, CA" \
  --pretty
```

## Actionability evaluation

```bash
cd autoresearch
python evaluate_actionability.py --case 1 --verbose
python evaluate_actionability.py --tier ENOUGH
python evaluate_actionability.py --all
```

## Legacy evaluator test

To test against the original immutable evaluator without permanently replacing the old harness:

```bash
cd autoresearch
cp research.py research_legacy_backup.py
cp research_case_graph.py research.py
python evaluate.py --case 1 --verbose
mv research_legacy_backup.py research.py
```

## Firecrawl

Firecrawl extraction is optional and off by default to avoid credit burn.

```bash
export FIRECRAWL_API_KEY="..."
export FLAMEON_ENABLE_FIRECRAWL=1
```

Use Firecrawl only for public pages. This harness is not designed to bypass authentication, private Axon/Evidence.com portals, or protected evidence systems.

## Expected scoring behavior

The old `research_score` may initially go down because this fork is stricter: claims do not equal verified evidence.

The intended improvement is in the actionability metrics:

- higher identity-lock precision
- fewer false HIGH results for INSUFFICIENT cases
- more cases with concrete artifact URLs
- more cases with downloadable/transcript-ready artifacts
