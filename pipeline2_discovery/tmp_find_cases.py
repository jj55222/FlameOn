"""Temporary helper — scrape request 22-7 with wait so full doc list loads."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
from collections import defaultdict

load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Use waitFor to let JS render the full document list
url = 'https://sfdpa.nextrequest.com/requests/22-7'
print(f'Scraping {url} with wait...')
try:
    result = app.scrape(
        url,
        formats=['markdown'],
        wait_for=8000,  # wait 8 seconds for JS
        only_main_content=False,
    )
except TypeError:
    # Fallback if param names are different
    result = app.scrape(url, formats=['markdown'])

md = getattr(result, 'markdown', '') or ''
print(f'md length: {len(md)}')

doc_pattern = re.compile(r'\[([^\]]+)\]\((https://sfdpa\.nextrequest\.com/documents/\d+)\)')
matches = doc_pattern.findall(md)
print(f'Total doc matches: {len(matches)}')

# Show first 30 matches
for fn, du in matches[:30]:
    print(f'  {fn[:70]}')
