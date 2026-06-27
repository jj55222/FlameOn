# San Diego PD — SB16 / SB1421 / AB748 Video Case Catalog

**Source:** San Diego Police Department mandated-disclosures portal
**Front-end index:** https://www.sandiego.gov/police/data-transparency/mandated-disclosures/sb16-sb1421-ab748
**Files host (S3, by-key only — no listing):** https://sdpdsb1421.sandiego.gov
**Crawled:** 2026-06-27 via `discovered_cases/sdpd_harvest.py`
**Candidate data (with direct download URLs):** `discovered_cases/sdpd_candidates.json`

> All files are **directly downloadable** (bodycam/incident video, 911/radio audio, IA/OIS
> PDFs, photos) — per-case folders `Video/ Audio/ Documents/ Photos/`. You do **not** need to
> download everything; pick a case and pull its `media_files` from the JSON, or run
> `python discovered_cases/sdpd_harvest.py --download --category "<cat>"`.
> The S3 host 403s without a browser User-Agent (the crawler/JSON URLs already handle this).

## Totals

- **50 cases**, **253 video files**, 649 audio files, 54 documents, 16 photos
- By category: Sustained Findings (16), AB 748 (13), Officer Involved Shootings (12), Use of Force (9)

## All cases (ranked by artifact richness)

| # | Score | Case | Category | Vid | Aud | Doc | Browse |
|---|------|------|----------|-----|-----|-----|--------|
| 1 | 43 | 01-13-2025 1150 E Street | Officer Involved Shootings | 3 | 23 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-13-2025%201150%20E%20Street&cat=Officer%20Involved%20Shootings) |
| 2 | 43 | 05-19-2024 400 47th Street | Officer Involved Shootings | 5 | 37 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=05-19-2024%20400%2047th%20Street&cat=Officer%20Involved%20Shootings) |
| 3 | 43 | 06-04-2023 700 E. San Ysidro Boulevard | Officer Involved Shootings | 13 | 48 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=06-04-2023%20700%20E.%20San%20Ysidro%20Boulevard&cat=Officer%20Involved%20Shootings) |
| 4 | 43 | 08-02-2023 7200 Mesa College Circle | Officer Involved Shootings | 5 | 62 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-02-2023%207200%20Mesa%20College%20Circle&cat=Officer%20Involved%20Shootings) |
| 5 | 43 | 08-28-2023 500 Iona Drive | Officer Involved Shootings | 4 | 74 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-28-2023%20500%20Iona%20Drive&cat=Officer%20Involved%20Shootings) |
| 6 | 40 | 01-17-2023 La Cresta Blvd, El Cajon | Officer Involved Shootings | 11 | 35 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-17-2023%20La%20Cresta%20Blvd%2C%20El%20Cajon&cat=Officer%20Involved%20Shootings) |
| 7 | 40 | 08-11-2023 3400 Lebon Drive | Officer Involved Shootings | 70 | 41 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-11-2023%203400%20Lebon%20Drive&cat=Officer%20Involved%20Shootings) |
| 8 | 40 | 08-15-2023 6100 El Cajon Blvd | Officer Involved Shootings | 9 | 50 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-15-2023%206100%20El%20Cajon%20Blvd&cat=Officer%20Involved%20Shootings) |
| 9 | 40 | 11-13-2023 3800 41st Street | Officer Involved Shootings | 3 | 14 | 3 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=11-13-2023%203800%2041st%20Street&cat=Officer%20Involved%20Shootings) |
| 10 | 40 | 12-07-2023 10500 4S Commons Drive | Officer Involved Shootings | 10 | 63 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=12-07-2023%2010500%204S%20Commons%20Drive&cat=Officer%20Involved%20Shootings) |
| 11 | 37 | 01-20-2023 Logan Avenue | Officer Involved Shootings | 11 | 28 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-20-2023%20Logan%20Avenue&cat=Officer%20Involved%20Shootings) |
| 12 | 36 | 07-06-2024 5400 La Jolla Blvd | Officer Involved Shootings | 3 | 55 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=07-06-2024%205400%20La%20Jolla%20Blvd&cat=Officer%20Involved%20Shootings) |
| 13 | 33 | 01-05-2025 4400 Fanuel Street | Use of Force | 5 | 4 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-05-2025%204400%20Fanuel%20Street&cat=Use%20of%20Force) |
| 14 | 33 | 10-16-2023 1400 National Ave | Use of Force | 2 | 2 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=10-16-2023%201400%20National%20Ave&cat=Use%20of%20Force) |
| 15 | 29 | 02-23-2024 4800 Savannah Street | Use of Force | 5 | 6 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=02-23-2024%204800%20Savannah%20Street&cat=Use%20of%20Force) |
| 16 | 29 | 02-09-2023 IA 2023-0027 | Sustained Findings | 2 | 2 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=02-09-2023%20IA%202023-0027&cat=Sustained%20Findings) |
| 17 | 29 | 08-15-2023 IA 2023-0388 | Sustained Findings | 11 | 8 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-15-2023%20IA%202023-0388&cat=Sustained%20Findings) |
| 18 | 26 | 01-01-2024 IA 2024-002 | Sustained Findings | 4 | 4 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-01-2024%20IA%202024-002&cat=Sustained%20Findings) |
| 19 | 26 | 06-06-2024 IA 2024-013 | Sustained Findings | 17 | 10 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=06-06-2024%20IA%202024-013&cat=Sustained%20Findings) |
| 20 | 26 | 01-18-2023 IA 2023-0028 | Sustained Findings | 2 | 4 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-18-2023%20IA%202023-0028&cat=Sustained%20Findings) |
| 21 | 26 | 02-16-2023 IA 2023-0124 | Sustained Findings | 3 | 3 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=02-16-2023%20IA%202023-0124&cat=Sustained%20Findings) |
| 22 | 26 | 05-13-2023 IA 2023-0225 | Sustained Findings | 4 | 3 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=05-13-2023%20IA%202023-0225&cat=Sustained%20Findings) |
| 23 | 26 | 08-20-2023 IA 2023-009 | Sustained Findings | 1 | 3 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=08-20-2023%20IA%202023-009&cat=Sustained%20Findings) |
| 24 | 26 | 10-25-2023 IA 2023-016 | Sustained Findings | 1 | 9 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=10-25-2023%20IA%202023-016&cat=Sustained%20Findings) |
| 25 | 26 | 11-02-2023 4300 44th Street | Use of Force | 5 | 4 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=11-02-2023%204300%2044th%20Street&cat=Use%20of%20Force) |
| 26 | 26 | 12-10-2021 IA 2023-0204 | Sustained Findings | 1 | 1 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=12-10-2021%20IA%202023-0204&cat=Sustained%20Findings) |
| 27 | 25 | 04-02-2024 IA 2024-0123 | Sustained Findings | 2 | 4 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=04-02-2024%20IA%202024-0123&cat=Sustained%20Findings) |
| 28 | 25 | 06-22-2023 IA 2023-0345 | Sustained Findings | 8 | 14 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=06-22-2023%20IA%202023-0345&cat=Sustained%20Findings) |
| 29 | 20 | 07-02-2024 1300 Russ Blvd | Use of Force | 8 | 0 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=07-02-2024%201300%20Russ%20Blvd&cat=Use%20of%20Force) |
| 30 | 20 | 07-04-2023 1300 East Mission Bay Drive | Use of Force | 2 | 0 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=07-04-2023%201300%20East%20Mission%20Bay%20Drive&cat=Use%20of%20Force) |
| 31 | 17 | 01-26-2025 560 5th Street | Use of Force | 2 | 0 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-26-2025%20560%205th%20Street&cat=Use%20of%20Force) |
| 32 | 17 | 03-07-2025 1400 4th Avenue | Use of Force | 3 | 0 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=03-07-2025%201400%204th%20Avenue&cat=Use%20of%20Force) |
| 33 | 17 | 06-09-2024 4300 Winona Avenue | Use of Force | 5 | 0 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=06-09-2024%204300%20Winona%20Avenue&cat=Use%20of%20Force) |
| 34 | 16 | 07-06-2024 IA 2024-012 | Sustained Findings | 0 | 9 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=07-06-2024%20IA%202024-012&cat=Sustained%20Findings) |
| 35 | 16 | 02-04-2023 IA 2023-003 | Sustained Findings | 0 | 5 | 1 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=02-04-2023%20IA%202023-003&cat=Sustained%20Findings) |
| 36 | 16 | 02-23-2023 IA 2023-004 | Sustained Findings | 0 | 9 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=02-23-2023%20IA%202023-004&cat=Sustained%20Findings) |
| 37 | 16 | 10-16-2023 IA 2023-012 | Sustained Findings | 0 | 15 | 2 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=10-16-2023%20IA%202023-012&cat=Sustained%20Findings) |
| 38 | 12 | 01-21-2026 2900 Balboa | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-21-2026%202900%20Balboa&cat=AB%20748) |
| 39 | 12 | 01-22-2026 Jamacha Road | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-22-2026%20Jamacha%20Road&cat=AB%20748) |
| 40 | 12 | 01-13-2025 1100 E street | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-13-2025%201100%20E%20street&cat=AB%20748) |
| 41 | 12 | 05-19-2024 47th Street | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=05-19-2024%2047th%20Street&cat=AB%20748) |
| 42 | 12 | 05-20-2024 Friars Road | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=05-20-2024%20Friars%20Road&cat=AB%20748) |
| 43 | 12 | 07-06-2024 5495 La Jolla Blvd | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=07-06-2024%205495%20La%20Jolla%20Blvd&cat=AB%20748) |
| 44 | 6 | 01-08-2025 1100 Kettner Blvd | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=01-08-2025%201100%20Kettner%20Blvd&cat=AB%20748) |
| 45 | 6 | 03-25-25 11600 Angelique | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=03-25-25%2011600%20Angelique&cat=AB%20748) |
| 46 | 6 | 04-04-2025 200 31st Street | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=04-04-2025%20200%2031st%20Street&cat=AB%20748) |
| 47 | 6 | 04-04-2025 Dawes Street | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=04-04-2025%20Dawes%20Street&cat=AB%20748) |
| 48 | 6 | 04-14-2025 26th and E | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=04-14-2025%2026th%20and%20E&cat=AB%20748) |
| 49 | 6 | 05-31-2025 Bermuda Avenue | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=05-31-2025%20Bermuda%20Avenue&cat=AB%20748) |
| 50 | 6 | 09-23-2025 5100 Orange Ave | AB 748 | 1 | 0 | 0 | [link](https://www.sandiego.gov/police/data-transparency/mandated-disclosures/case?id=09-23-2025%205100%20Orange%20Ave&cat=AB%20748) |

## Top bundles (richest video+audio+doc cases — best pipeline candidates)

- **08-11-2023 3400 Lebon Drive** (Officer Involved Shootings) — 70 video, 41 audio, 1 doc  ·  case_id `sdpd_08_11_2023_3400_lebon_drive`
- **06-06-2024 IA 2024-013** (Sustained Findings) — 17 video, 10 audio, 1 doc  ·  case_id `sdpd_06_06_2024_ia_2024_013`
- **06-04-2023 700 E. San Ysidro Boulevard** (Officer Involved Shootings) — 13 video, 48 audio, 2 doc  ·  case_id `sdpd_06_04_2023_700_e_san_ysidro_boulevard`
- **01-17-2023 La Cresta Blvd, El Cajon** (Officer Involved Shootings) — 11 video, 35 audio, 2 doc  ·  case_id `sdpd_01_17_2023_la_cresta_blvd_el_cajon`
- **01-20-2023 Logan Avenue** (Officer Involved Shootings) — 11 video, 28 audio, 1 doc  ·  case_id `sdpd_01_20_2023_logan_avenue`
- **08-15-2023 IA 2023-0388** (Sustained Findings) — 11 video, 8 audio, 1 doc  ·  case_id `sdpd_08_15_2023_ia_2023_0388`
- **12-07-2023 10500 4S Commons Drive** (Officer Involved Shootings) — 10 video, 63 audio, 1 doc  ·  case_id `sdpd_12_07_2023_10500_4s_commons_drive`
- **08-15-2023 6100 El Cajon Blvd** (Officer Involved Shootings) — 9 video, 50 audio, 2 doc  ·  case_id `sdpd_08_15_2023_6100_el_cajon_blvd`

## How to pull a case

```bash
# whole category (e.g. just OIS), video-only cases, download media:
python discovered_cases/sdpd_harvest.py --category "Officer Involved Shootings" --require-video --download

# or grab one case's URLs from the JSON:
python3 -c "import json; d={c['case_id']:c for c in json.load(open('discovered_cases/sdpd_candidates.json'))}; \
  [print(m['url']) for m in d['sdpd_08_11_2023_3400_lebon_drive']['media_files']]"
```
