"""
Final attempt: probe sequential doc IDs near 13420842 using GET (not HEAD).
Should reveal the adjacent sibling files from the same upload batch.
"""
import requests
import re
import time

def probe(doc_id):
    url = f'https://sfdpa.nextrequest.com/documents/{doc_id}/download'
    try:
        # Stream + small timeout so we read headers without pulling MB of data
        r = requests.get(url, stream=True, timeout=10, allow_redirects=True,
                         headers={'User-Agent': 'Mozilla/5.0'})
        ct = r.headers.get('content-type', '')
        cd = r.headers.get('content-disposition', '')
        cl = r.headers.get('content-length', '0')
        fn_match = re.search(r'filename="([^"]+)"', cd)
        filename = fn_match.group(1) if fn_match else ''
        r.close()
        if filename:
            return r.status_code, filename, int(cl) if cl.isdigit() else 0
    except Exception as e:
        return None, f'ERR: {e}', 0
    return r.status_code, '', 0


print('Probing ID range 13420830-13420860 (near 0409-18 anchor)')
found = []
for did in range(13420830, 13420861):
    result = probe(did)
    if result and result[1] and '.' in result[1]:
        status, fn, size = result
        size_mb = size / 1024 / 1024 if size else 0
        print(f'  [{did}]  {status}  {size_mb:>6.1f} MB  {fn[:80]}')
        found.append((did, fn, size))
    time.sleep(0.3)

print(f'\nFound {len(found)} files')

# Also try ranges around 0164-18 and 0261-18 upload dates
# 0164-18 uploaded June 8, 2023 → ID around 14.2-14.5M
# 0261-18 uploaded March 21, 2023 → ID around 13.9-14.1M
# 0213-18 uploaded April 29, 2022 → ID around 12.7-13.3M

print('\nProbing 0261-18 range (13900000-13900030)...')
for did in range(13900000, 13900031):
    result = probe(did)
    if result and result[1] and '.' in result[1]:
        status, fn, size = result
        size_mb = size / 1024 / 1024 if size else 0
        print(f'  [{did}]  {status}  {size_mb:>6.1f} MB  {fn[:80]}')
    time.sleep(0.2)
