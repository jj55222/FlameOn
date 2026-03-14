# FlameOn
# ⚖️ Sunshine-Gated Intelligence Pipeline

An automated research system for True Crime production, specifically engineered to prioritize high-yield jurisdictions and verified legal outcomes.

## 🎯 Current Objectives
- **Sunshine Monitoring:** Automated tracking of 60+ official law enforcement channels in FL, AZ, TX, and OH.
- **Gated Validation:** Cursory "Validation Rounds" (Brave/Tavily) to confirm case resolution before triggering deep research.
- **Outcome-Driven Research:** Prioritizing cases with final sentences for high-narrative payoff (e.g., "Dr. Insanity" style).
- **API Efficiency:** Leveraging low-cost tools (Brave) for speculation and high-precision tools (Exa) for final asset mining.

## 🛠 Strategic Workflow
1. **Poll:** Check the `uploads` playlist of our Sunshine-state PD channels (YouTube API).
2. **Validate (Brave):** - Search: `"[Suspect Name]" [City] sentencing news`
   - Gate: If status != "Sentenced", the case is archived for re-check in 30 days.
3. **Research (Exa):** If [Sentenced == True], trigger neural search for Court Dockets, Sentencing Orders, and Interrogation PDFs.
4. **Collect:** Move verified assets to structured folders in Google Drive.

## 📂 Google Drive Hierarchy
- `/CrimeDoc-Pipeline/01-Leads-Pending/`
- `/CrimeDoc-Pipeline/02-Verified-Closed/` (Subfolders: `[State]/[Suspect_Name]/...`)
- `/CrimeDoc-Pipeline/03-Production-Ready/`
