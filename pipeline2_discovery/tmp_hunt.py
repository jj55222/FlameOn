"""
Option C (token-efficient):
Test if /documents?query= actually filters server-side in Firecrawl's rendered output.
If yes, we get each case's files in one scrape (~1 credit each).
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Try the /documents URL with search params that NextRequest uses on the client
# From the SF DPA page UI, searching "0213-18" would filter the list.
# The actual URL format used by the JS is likely via the NextRequest API — let's test.

test_urls = [
    # Try different URL parameter formats
    'https://sfdpa.nextrequest.com/documents?query=0213-18',
    'https://sfdpa.nextrequest.com/documents?search=0213-18',
    'https://sfdpa.nextrequest.com/documents?filename=0213-18',
    'https://sfdpa.nextrequest.com/documents?q=0213-18',
]

for url in test_urls:
    print(f'\n=== {url} ===')
    try:
        result = app.scrape(url, formats=['markdown'], max_age=0)
        md = getattr(result, 'markdown', '') or ''
        # Look for results count
        total_match = re.search(r'(\d+)\s*/\s*(\d+)\s*results', md)
        if total_match:
            print(f'  Results: {total_match.group(1)}/{total_match.group(2)}')
        # Count 0213-18 matches
        c = len(re.findall(r'0213-18', md))
        print(f'  "0213-18" occurrences in md: {c}')
        # If filter worked, we'd see far fewer overall entries
        # Print first file reference
        file_match = re.search(r'(0213-18[^\n]*\.(?:mp3|mp4|pdf))', md)
        if file_match:
            print(f'  First match: {file_match.group(1)[:80]}')
    except Exception as e:
        print(f'  ERR: {e}')
