# A/B Bundle Validation — file-by-file

Probed every file URL in tier A/B bundles (1-byte range, no downloads).

| source | bundles | files | live | dead | live % |
|--------|--------:|------:|-----:|-----:|-------:|
| chicago_copa | 1 | 15 | 15 | 0 | 100.0% |
| longbeach_laserfiche | 123 | 2813 | 2813 | 0 | 100.0% |
| muckrock | 12 | 410 | 410 | 0 | 100.0% |
| sdpd | 37 | 943 | 943 | 0 | 100.0% |
| sfdpa_nextrequest | 31 | 242 | 74 | 168 | 30.6% |
| **TOTAL** | 204 | 4423 | 4255 | 168 | 96.2% |

## Bundles with ≥1 dead file (26) — NOT fully valid

- [B] sfdpa_nextrequest · `sfdpa_0656_18` — 27/27 dead
- [B] sfdpa_nextrequest · `sfdpa_0627_15_woods` — 19/19 dead
- [B] sfdpa_nextrequest · `sfdpa_0651_13` — 18/18 dead
- [B] sfdpa_nextrequest · `sfdpa_0383_14` — 8/8 dead
- [B] sfdpa_nextrequest · `sfdpa_0213_18` — 8/8 dead
- [B] sfdpa_nextrequest · `sfdpa_0210_14` — 7/7 dead
- [B] sfdpa_nextrequest · `sfdpa_0040_15` — 7/7 dead
- [B] sfdpa_nextrequest · `sfdpa_0052_14` — 6/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0081_19` — 6/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0658_08` — 6/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0470_17` — 6/6 dead
- [B] sfdpa_nextrequest · `sfdpa_00048794_21` — 6/6 dead
- [B] sfdpa_nextrequest · `sfdpa_0409_18` — 5/5 dead
- [B] sfdpa_nextrequest · `sfdpa_0164_18` — 5/5 dead
- [B] sfdpa_nextrequest · `sfdpa_0261_18` — 5/5 dead
- [B] sfdpa_nextrequest · `sfdpa_0270_18` — 5/5 dead
- [B] sfdpa_nextrequest · `sfdpa_0438_10` — 4/4 dead
- [B] sfdpa_nextrequest · `sfdpa_0971_89` — 4/4 dead
- [B] sfdpa_nextrequest · `sfdpa_1451_87` — 4/4 dead
- [B] sfdpa_nextrequest · `sfdpa_00059678_24` — 3/3 dead
- [B] sfdpa_nextrequest · `sfdpa_0652_10` — 2/9 dead
- [B] sfdpa_nextrequest · `sfdpa_0265_17` — 2/2 dead
- [B] sfdpa_nextrequest · `sfdpa_00048961_21` — 2/2 dead
- [B] sfdpa_nextrequest · `sfdpa_0068_01` — 1/12 dead
- [B] sfdpa_nextrequest · `sfdpa_0141_19` — 1/8 dead
- [B] sfdpa_nextrequest · `sfdpa_44328_20` — 1/3 dead
