# A/B Bundle Validation — file-by-file

Probed every file URL in tier A/B bundles (1-byte range, no downloads).

| source | bundles | files | live | dead | live % |
|--------|--------:|------:|-----:|-----:|-------:|
| chicago_copa | 1 | 15 | 15 | 0 | 100.0% |
| longbeach_laserfiche | 123 | 2813 | 2813 | 0 | 100.0% |
| muckrock | 12 | 410 | 410 | 0 | 100.0% |
| sdpd | 37 | 943 | 943 | 0 | 100.0% |
| sfdpa_nextrequest | 31 | 242 | 225 | 17 | 93.0% |
| **TOTAL** | 204 | 4423 | 4406 | 17 | 99.6% |

## Bundles with ≥1 dead file (5) — NOT fully valid

- [B] sfdpa_nextrequest · `sfdpa_0651_13` — 7/18 dead
- [B] sfdpa_nextrequest · `sfdpa_0656_18` — 4/27 dead
- [B] sfdpa_nextrequest · `sfdpa_0658_08` — 3/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0081_19` — 2/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0652_10` — 1/9 dead
