# NEXT CUTS — ledger-backed menu (auto-refreshed nightly by production_promoter v2)

RULES (binding):
1. Select ONLY from this file. Local outputs/ or side baskets are NOT feedstock.
2. BEFORE cutting, check the ledger:  python .tmp/preprocess_queue/produced_ledger.py --check <case_id>
   (--mark now refreshes this menu immediately, but a --check right before cutting is still
   the authority — this file can be minutes stale, the ledger never is.)
3. AFTER mastering, register it:      python .tmp/preprocess_queue/produced_ledger.py --mark <case_id> --scope longform|shorts --evidence <path>
4. Recuts of anything the ledger marks done require an explicit operator request.
5. If READY cannot satisfy the requested quantity, DO NOT substitute a familiar case.
   Hydrate from the BENCH below (one command per case) or say the menu is short —
   re-picking an already-produced or already-rejected case is the worst outcome.
6. HARD GATE for any longform pick: usable BWC of the central event. Dashcam, interviews,
   911 and documents strengthen a BWC case; they never replace it.

## LONGFORM — ready now (media on disk)
- **sacso_23_295175** — **ORDERED 2026-08-09, EDP ISSUED** · cut from `EDP_sacso_23_295175_20260809.md`, not from this line · 23 media + 5 PDFs (incl. Use of Force Report) · K9 apprehension frame-verified on BWC-3 · format call 6-7 min
- ~~sfdpa_45130_20~~ — **WITHDRAWN 2026-08-09: ALREADY PRODUCED** (ledger: longform + shorts done 2026-08-05). Do not cut. EDP retained as a possible-recut record only; a recut needs an explicit operator request (rule 4).
- **sacso_23_295175** (rank 76.5) — 23 media + 5 PDFs on disk · card .tmp/sacso_23_295175/CASE_CARD.md · transcripts .tmp/sacso_23_295175/d2/transcripts/
- **longbeach_officer_involved_shootings_ois_november_2024_5200_atlantic_ave** (rank 63.0) — 126 media + 13 PDFs on disk · card .tmp/longbeach_officer_involved_shootings_ois_november_2024_5200_atlantic_ave/CASE_CARD.md · transcripts .tmp/longbeach_officer_involved_shootings_ois_november_2024_5200_atlantic_ave/d2/transcripts/
- **copa_2021_0001112** (rank 60.0) — 12 media + 9 PDFs on disk · card .tmp/copa_2021_0001112/CASE_CARD.md · transcripts .tmp/copa_2021_0001112/d2/transcripts/
- **copa_2021_0001076** (rank 60.0) — 32 media + 10 PDFs on disk · card .tmp/copa_2021_0001076/CASE_CARD.md · transcripts .tmp/copa_2021_0001076/d2/transcripts/
- **copa_2020_0003466** (rank 60.0) — 12 media + 17 PDFs on disk · card .tmp/copa_2020_0003466/CASE_CARD.md · transcripts .tmp/copa_2020_0003466/d2/transcripts/
- **copa_2021_0001161** (rank 60.0) — 12 media + 8 PDFs on disk · card .tmp/copa_2021_0001161/CASE_CARD.md · transcripts .tmp/copa_2021_0001161/d2/transcripts/
- **copa_2022_0003054** (rank 58.2) — 8 media + 10 PDFs on disk · card .tmp/copa_2022_0003054/CASE_CARD.md · transcripts .tmp/copa_2022_0003054/d2/transcripts/
- **sfdpa_0656_18** (rank 46.8) — 1 media + 4 PDFs on disk · card .tmp/sfdpa_0656_18/CASE_CARD.md · transcripts .tmp/sfdpa_0656_18/d2/transcripts/
- **sfdpa_0045_19** (rank 44.8) — 2 media + 4 PDFs on disk · card .tmp/sfdpa_0045_19/CASE_CARD.md · transcripts .tmp/sfdpa_0045_19/d2/transcripts/
- **ohbci_demond_eskridge** (rank 0) — 10 media + 66 PDFs on disk · card .tmp/ohbci_demond_eskridge/CASE_CARD.md · transcripts .tmp/ohbci_demond_eskridge/d2/transcripts/
- **sacpd_officer_involved_shooting_2000_block_of_1st___4ekvo** (rank 0) — 10 media + 0 PDFs on disk · card .tmp/sacpd_officer_involved_shooting_2000_block_of_1st___4ekvo/CASE_CARD.md · transcripts .tmp/sacpd_officer_involved_shooting_2000_block_of_1st___4ekvo/d2/transcripts/
- **denpd_6_14_2025_w_colfax_ave_and_n_bannock_st_protes** (rank 0) — 13 media + 0 PDFs on disk · card .tmp/denpd_6_14_2025_w_colfax_ave_and_n_bannock_st_protes/CASE_CARD.md · transcripts .tmp/denpd_6_14_2025_w_colfax_ave_and_n_bannock_st_protes/d2/transcripts/
- **sacpd_20_281085_uof_resulting_in_gbi_2800_block_of_psnss-** (rank 0) — 4 media + 0 PDFs on disk · card .tmp/sacpd_20_281085_uof_resulting_in_gbi_2800_block_of_psnss-/CASE_CARD.md · transcripts .tmp/sacpd_20_281085_uof_resulting_in_gbi_2800_block_of_psnss-/d2/transcripts/

## BENCH — unused, transcript-complete, one command to hydrate (top 15 of 22)
Hydrate, wait for the card to regen, then it appears in READY on the next promoter pass:
```
zsh .tmp/preprocess_queue/hydrate_production.sh <case_id>
```

- longbeach_officer_involved_shootings_ois2018_002_perez_luis_on_or_after_9_13_23 (rank 88.7, confrontation 6)
- sacso_16_24127 (rank 86.5, confrontation 3)
- sacso_15_232203 (rank 81.9, confrontation 3)
- sacso_20_67422 (rank 69.0, confrontation 172)
- sacso_21_018130 (rank 69.0, confrontation 16)
- sacso_18_446660 (rank 63.0, confrontation 3)
- sacso_17_367521 (rank 63.0, confrontation 2)
- sacso_19_427375 (rank 62.4, confrontation 3)
- sacso_19_357898 (rank 61.0, confrontation 3)
- sacso_15_65065 (rank 60.8, confrontation 3)
- sacso_17_239317 (rank 59.6, confrontation 5)
- sacso_2025psb_713 (rank 59.1, confrontation 1)
- muckrock_78826_officer_involved_shooting_of_mark_johnso (rank 58.8, confrontation 22)
- sfdpa_0141_19 (rank 43.2, confrontation 1)
- denpd_6_22_2020_e_35th_place_sable_street_officer_in (rank 0, confrontation 18)

## SHORTS-ready (shorts lane only — longform may be separately done)
