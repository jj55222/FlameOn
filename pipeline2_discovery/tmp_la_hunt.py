"""Get full Mesa PD Community Briefings page content with case titles."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

url = 'https://www.mesaaz.gov/Public-Safety/Mesa-Police/Community/Transparency-In-Policing/Community-Briefings'
result = app.scrape(url, formats=['markdown'])
md = getattr(result, 'markdown', '') or ''

with open('mesa_briefings.md', 'w', encoding='utf-8') as f:
    f.write(md)

# Find every (title, youtube_url) pair
# Mesa often formats as: "### Critical Incident Briefing - Month Day, YYYY"
# followed by a description and a youtube link

# Extract all sections with YouTube links
section_pattern = re.compile(r'(#{2,4}\s+[^\n]+?)(?=\n)(.*?)(?=\n#{2,4}\s+|$)', re.DOTALL)
sections = section_pattern.findall(md)
print(f'Found {len(sections)} sections')

yt_pattern = re.compile(r'(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})')

cases = []
for header, body in sections:
    # Find YouTube video IDs
    vids = list(set(yt_pattern.findall(body)))
    if not vids:
        continue
    # Skip cases with the word 'protest' in header or body
    content = (header + ' ' + body).lower()
    if any(kw in content for kw in ['protest', 'demonstration', 'protester', 'rally']):
        continue
    cases.append({
        'header': header.strip().lstrip('#').strip(),
        'video_ids': vids,
        'body': body[:600],
    })

print(f'\nCases (non-protest, with video):')
print('=' * 80)
for i, c in enumerate(cases[:25], 1):
    print(f'\n[{i}] {c["header"]}')
    for vid in c['video_ids']:
        print(f'   https://www.youtube.com/watch?v={vid}')
    print(f'   {c["body"][:300].strip()}')
