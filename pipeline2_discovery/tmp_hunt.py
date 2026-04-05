"""
Fresh scrape of 22-7 with only_main_content=False to get ALL timeline entries.
Earlier scrapes returned 3-7KB, but my very first scrape got 14KB+ with full list.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Use only_main_content=False + cache bust
print('Scraping 22-7 with only_main_content=False...')
result = app.scrape(
    'https://sfdpa.nextrequest.com/requests/22-7',
    formats=['markdown'],
    only_main_content=False,
    wait_for=5000,
    max_age=0,
)
md = getattr(result, 'markdown', '') or ''
print(f'md: {len(md)} chars')

# Save for inspection
with open('request_22-7_full.md', 'w', encoding='utf-8') as f:
    f.write(md)

# Extract all filenames referenced
filenames = set(re.findall(r'([\d]{3,5}-[\d]{2,4}[^\n<>]*?\.(?:mp3|mp4|mov|pdf|docx))', md, re.IGNORECASE))
filenames |= set(re.findall(r'(Production[^\n<>]*?\.pdf)', md))
filenames |= set(re.findall(r'([\w ]+?\.(?:mp3|mp4|mov))', md))

print(f'Filenames referenced: {len(filenames)}')
for f in sorted(filenames):
    print(f'  {f[:80]}')

# Also count [text](url) pairs for each file
link_pattern = re.compile(r'\[([^\]]+)\]\((https://[^)]+)\)')
links = link_pattern.findall(md)
print(f'\nTotal markdown links: {len(links)}')
# Filter for document links
doc_links = [(t, u) for t, u in links if '/documents/' in u]
print(f'Document links: {len(doc_links)}')
for t, u in doc_links[:10]:
    print(f'  [{t[:60]}]({u})')
