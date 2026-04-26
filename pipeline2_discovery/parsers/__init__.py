"""
parsers/ — per-agency CIB/OIS publishing-page parsers.

Each module exposes a function `fetch_and_parse() -> list[dict]` returning
incident records with this minimum schema:

    {
      "agency": str,                   # e.g. "LAPD"
      "incident_id": str,              # agency-internal id, e.g. "F015-23"
      "incident_date": str,            # YYYY-MM-DD if known, else ""
      "incident_type": str,            # "OIS", "use_of_force", "in_custody_death", etc.
      "division": str,                 # geographic/precinct context (optional)
      "location": str,                 # address or city
      "subjects": [str],               # named subjects (defendant, suspect, decedent)
      "video_urls": [str],             # bodycam/CIB video URLs
      "documents": [str],              # PDFs, news posts, supplemental docs
      "summary": str,                  # one-line description
      "source_url": str,               # the agency page this came from
      "scraped_at": str,               # ISO timestamp
    }

A central cache (cib_cache/{agency}.json) holds these records. research.py
includes a `search_cib_cache(name, jurisdiction)` lookup that mirrors how
search_portal_cache works — zero API cost per case.
"""
