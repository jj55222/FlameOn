"""Debug: see what the HTML structure looks like for file links."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

result = app.scrape('https://sfdpa.nextrequest.com/requests/22-7', formats=['html'])
html = getattr(result, 'html', '') or ''
print(f'html: {len(html)} chars')

# Find where 0164-18 first appears
idx = html.find('0164-18')
if idx >= 0:
    print(f'\n0164-18 found at position {idx}, surrounding HTML:')
    print(html[max(0, idx-500):idx+1500])
else:
    print('0164-18 NOT in html!')

# Also search for any .mp4 reference
mp4_matches = re.findall(r'[^\s\"\\\'<>]*\.mp4', html, re.IGNORECASE)
print(f'\nAll .mp4 references: {len(mp4_matches)}')
for m in mp4_matches[:10]:
    print(f'  {m}')
