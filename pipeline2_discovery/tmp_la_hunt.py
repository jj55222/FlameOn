"""
Probe JSO Transparency Portal — Jacksonville Sheriff publishes BWC
for major crimes proactively (not FOIA-based). This should have real
criminal incidents with bodycam footage.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# JSO transparency portal
url = 'https://transparency.jaxsheriff.org/'
print(f'Scraping: {url}')
result = app.scrape(url, formats=['markdown', 'links'])
md = getattr(result, 'markdown', '') or ''
links = getattr(result, 'links', []) or []

print(f'md: {len(md)} chars, links: {len(links)}')
print()
print('--- FIRST 4000 CHARS ---')
print(md[:4000])
print()
print('--- LINKS ---')
for l in links[:30]:
    if isinstance(l, str):
        print(f'  {l}')
