"""
Parse HTML of request 22-7 carefully.
Earlier output showed filenames inside <p class="wrap-text quill-content">
as PLAIN TEXT separated by <br>. But the HTML must also contain the document
IDs somewhere - maybe in data- attributes, or rendered by JS on click.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Try with wait_for + actions to let JS render the anchor tags
print('Scraping 22-7 with wait + actions...')
result = app.scrape(
    'https://sfdpa.nextrequest.com/requests/22-7',
    formats=['html'],
    wait_for=10000,
    actions=[
        {'type': 'wait', 'milliseconds': 3000},
        {'type': 'scroll', 'direction': 'down'},
        {'type': 'wait', 'milliseconds': 2000},
        {'type': 'scroll', 'direction': 'down'},
        {'type': 'wait', 'milliseconds': 2000},
        {'type': 'scroll', 'direction': 'down'},
        {'type': 'wait', 'milliseconds': 3000},
    ],
)
html = getattr(result, 'html', '') or ''
print(f'html: {len(html)} chars')

# Find all /documents/NNNN references
doc_ids = re.findall(r'/documents/(\d+)', html)
print(f'Total /documents/N references: {len(doc_ids)} ({len(set(doc_ids))} unique)')
print(f'Unique IDs: {sorted(set(doc_ids))}')

# Save HTML to inspect
with open('request_22-7.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('Saved request_22-7.html')

# Look for any mention of 0213 / 0164 / 0261 with a nearby doc ID
for case_id in ['0213-18', '0164-18', '0261-18', '0409-18']:
    print(f'\n=== {case_id} ===')
    for m in re.finditer(re.escape(case_id), html):
        pos = m.start()
        # Look for /documents/ in surrounding window
        window = html[max(0, pos-500):pos+500]
        ids_nearby = re.findall(r'/documents/(\d+)', window)
        if ids_nearby:
            print(f'  Position {pos}: nearby doc IDs {set(ids_nearby)}')
            break
