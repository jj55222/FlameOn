"""Direct probe for known case files that appeared in request 22-7 timeline."""
import requests
import re

# From the earlier scrape of request 22-7 I saw these case folders mentioned with docs:
# 0164-18: 2 BWC + 2 DPA interviews
# 0261-18: 2 BWC + 2 DPA interviews
# 0270-18: BWC + 2 surveillance + PDFs
# 48794-21: 2 BWC + PDFs

# NextRequest /documents URLs use auto-incrementing IDs. Let me search the /documents
# index with a filter query via URL param to find these specific cases by folder name

base = 'https://sfdpa.nextrequest.com/documents'
queries = [
    ('0261-18', base + '?query=0261-18'),
    ('0164-18', base + '?query=0164-18'),
    ('0270-18', base + '?query=0270-18'),
    ('48794-21', base + '?query=48794-21'),
    ('BWC', base + '?query=BWC'),
]

from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

for name, url in queries:
    print(f'\n=== query: {name} ===')
    try:
        result = app.scrape(url, formats=['markdown'])
        md = getattr(result, 'markdown', '') or ''
        # Find total
        total_match = re.search(r'(\d+)\s*/\s*\d+\s*results', md)
        if total_match:
            print(f'  Results: {total_match.group(0)}')
        # Parse rows
        row_pattern = re.compile(
            r'\[([^\]]+)\]\((https://[^)]*/documents/\d+)\)\s*\|\s*'
            r'\[([\w\-]+)\]\((https://[^)]*/requests/[\w\-]+)\)\s*\|\s*'
            r'([\d/]+)\s*\|\s*(\d+)\s*\|\s*([^|]*)\|'
        )
        docs = list(row_pattern.finditer(md))
        print(f'  Parsed docs: {len(docs)}')
        for m in docs[:20]:
            fn = m.group(1).strip()
            du = m.group(2).strip()
            print(f'    {fn[:75]}')
            print(f'      {du}/download')
    except Exception as e:
        print(f'  ERR: {e}')
