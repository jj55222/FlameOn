# Case Bundle Vet Report

- Mode: classification/tier dry run
- Candidate files rewritten: no
- Generated: 2026-06-28T22:42:18+00:00

| source | bundles | files | live | dead | unknown | A | B | C | D |
|--------|--------:|------:|-----:|-----:|--------:|--:|--:|--:|--:|
| chicago_copa | 2131 | 4888 | 0 | 0 | 4888 | 0 | 2 | 0 | 2129 |
| longbeach_laserfiche | 443 | 12225 | 0 | 0 | 12225 | 0 | 123 | 320 | 0 |
| muckrock | 53 | 702 | 0 | 0 | 702 | 0 | 12 | 16 | 25 |
| sdpd | 50 | 956 | 0 | 0 | 956 | 28 | 9 | 0 | 13 |
| sfdpa_nextrequest | 95 | 397 | 0 | 0 | 397 | 0 | 31 | 0 | 64 |

## Notes

### chicago_copa
- 2129 bundle(s): COPA case has no captured video in candidate file; run copa_harvest.py --capture-vimeo to enrich.

### longbeach_laserfiche
- 307 bundle(s): Laserfiche mediahandler URL replaced with ElectronicFile download_url where available.
