"""
Probe sequential doc IDs near known anchors to find gold case files.
Known anchor: 13420842 = 0409-18 BWC of Officer Sherry.
NextRequest assigns sequential IDs, so sibling files (same timeline batch)
should be adjacent.
"""
import requests
import re
import json
import time

# Known anchors (from foia_docs_cache.json)
# 0409-18 Sherry BWC = 13420842
# 0409-18 Production PDF = 13420843
# Case 0213-18 upload dates are April 29 2022 (Ex.M-1 Galande doc 13224767)
# Case 0164-18 files uploaded June 8 2023
# Case 0261-18 files uploaded March 21 2023

# Strategy: probe ranges and check HEAD for files
def check_doc(doc_id):
    """Return (filename, size, type) or None."""
    url = f'https://sfdpa.nextrequest.com/documents/{doc_id}/download'
    try:
        r = requests.head(url, timeout=15, allow_redirects=True,
                          headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return None
        cd = r.headers.get('content-disposition', '')
        fn_match = re.search(r'filename="([^"]+)"', cd)
        filename = fn_match.group(1) if fn_match else ''
        size = int(r.headers.get('content-length', 0))
        ct = r.headers.get('content-type', '')
        return filename, size, ct
    except Exception as e:
        return None

# Probe a range around known 0164-18 anchor (13420842 for 0409-18 Sherry)
# Case 0164-18 was uploaded June 2023 so should be near 14M-14.5M range
# Case 0261-18 was uploaded March 2023 so should be near 13.5M-14M
# Case 0213-18 uploaded April 2022 so should be around 12.5-13.3M

print('Probing doc ID ranges near known anchors...')
print()

# Start with range around 0409-18 and walk outward
anchor = 13420842  # 0409-18 Sherry
results = {}

# Walk backwards and forwards 20 IDs
for offset in list(range(-30, 31)):
    did = anchor + offset
    info = check_doc(did)
    if info:
        fn, size, ct = info
        sz_mb = size / 1024 / 1024
        if any(ext in fn.lower() for ext in ['.mp3', '.mp4', '.mov', '.pdf']):
            print(f'  [{did}]  {sz_mb:>6.1f} MB  {fn[:70]}')
            results[did] = fn
    time.sleep(0.2)

print()
print(f'Found {len(results)} documents in range {anchor-30}..{anchor+30}')

# Save
with open('doc_id_probe.json', 'w') as f:
    json.dump({str(k): v for k, v in results.items()}, f, indent=2)
