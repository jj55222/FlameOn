"""Test NextRequest's JSON API directly."""
import requests
import json

# NextRequest has a public JSON API — typically at /api/v2/requests/{id}/documents
# or similar. Let's probe common endpoints.
base = 'https://sfdpa.nextrequest.com'
endpoints = [
    '/client/request/22-7',
    '/api/v2/requests/22-7',
    '/api/requests/22-7',
    '/api/v2/requests/22-7/documents',
    '/api/requests/22-7/documents',
    '/requests/22-7.json',
    '/client/request/22-7.json',
    '/api/v2/public_requests/22-7',
    '/documents.json',
    '/documents/search.json',
]

headers = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
}

for ep in endpoints:
    url = base + ep
    try:
        r = requests.get(url, headers=headers, timeout=10, allow_redirects=False)
        ct = r.headers.get('content-type', '')
        print(f'  {r.status_code}  {ct[:40]:40s}  {ep}')
        if r.status_code == 200 and 'json' in ct.lower():
            data = r.json()
            if isinstance(data, dict):
                print(f'    Keys: {list(data.keys())[:10]}')
            elif isinstance(data, list):
                print(f'    List of {len(data)} items')
                if data:
                    print(f'    First: {str(data[0])[:200]}')
    except Exception as e:
        print(f'  ERR {ep}: {str(e)[:60]}')
