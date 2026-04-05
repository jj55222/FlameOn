"""Inspect what /requests/22-7.json and /documents.json actually return."""
import requests

headers = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
}

for url in [
    'https://sfdpa.nextrequest.com/requests/22-7.json',
    'https://sfdpa.nextrequest.com/documents.json',
]:
    print(f'\n=== {url} ===')
    r = requests.get(url, headers=headers, timeout=15)
    print(f'Status: {r.status_code}')
    print(f'Content-Length: {len(r.content)}')
    print(f'First 500 chars:')
    print(r.text[:500])
