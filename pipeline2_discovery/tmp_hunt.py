"""
Resolve filenames → document URLs for case 0213-18 and other gold cases.
Strategy: scrape deep pages of /documents global index until we find all target files.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
import time
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

TARGET_FILENAMES = {
    # Case 0213-18
    '0213-18 - BWC of Lt. Christopher Wilhelm #1282.mp4',
    '0213-18 - DPA Interview of Officer Christopher Wilhelm #1282.mp3',
    '0213-18 - DPA Interview of Officer Eric Robinson #2350 - Part 1.mp3',
    '0213-18 - DPA Interview of Officer Eric Robinson #2350 - Part 2.mp3',
    '0213-18 - DPA Interview of Sgt. Matthew Loya #352.mp3',
    '0213-18 - Surveillance Footage.mp4',
    'Production - 0213-18.pdf',
    # Case 0164-18
    '0164-18 BWC of Ofc. Christopher Prescott #1605 - Redacted.mp4',
    '0164-18 BWC of Ofc. Frank Ocolmendy #2191 - Redacted.mp4',
    '0164-18 DPA Interview of Ofc. Christopher Prescott #1605 - Redacted.mp3',
    '0164-18 DPA Interview of Ofc. Frank Olcomendy #2191 - Redacted.mp3',
    'Production - 0164-18.pdf',
    # Case 0261-18
    '0261-18 BWC of Ofc. Brian Burke #32 - Redacted.mp4',
    '0261-18 BWC of Ofc. Erik Risslen #381 - Redacted.mp4',
    '0261-18 DPA Interview of Ofc. Brian Burke #32 - Redacted.mp3',
    '0261-18 DPA Interview of Ofc. Erik Risslen #32 - Redacted.mp3',
    'Production - 0261-18.pdf',
    # Case 0409-18
    '0409-18 BWC of Officer Bradford #4199.mp4',
    '0409-18 BWC of Officer Sherry #1046 - Redacted.mp4',
    '0409-18 DPA Interview of Officer Sherry #1046 - Redacted.mp3',
    '0409-18 DPA Interview of Sergeant Bradford #4199 - Redacted.mp3',
    'Production - 0409-18.pdf',
}

# Fuzzy match helper: strip " - Redacted", "Part X", extensions for comparison
def normalize(s):
    s = s.lower()
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

found = {}  # filename -> doc_url

# Scrape many pages of /documents
for page in range(1, 15):
    url = 'https://sfdpa.nextrequest.com/documents' if page == 1 else f'https://sfdpa.nextrequest.com/documents?page={page}'
    print(f'  page {page}...', end=' ', flush=True)
    try:
        result = app.scrape(url, formats=['html'], max_age=0)
        html = getattr(result, 'html', '') or ''

        # Match: <a href="/documents/12345">filename</a>
        link_pattern = re.compile(r'<a[^>]*href="(/documents/\d+)"[^>]*>([^<]+)</a>', re.IGNORECASE)
        matches = link_pattern.findall(html)
        print(f'{len(matches)} links', end=' ')

        new_hits = 0
        for doc_path, raw_name in matches:
            # Decode HTML entities and normalize
            clean_name = raw_name.strip()
            clean_name = clean_name.replace('&#39;', "'").replace('&amp;', '&').replace('&quot;', '"')

            # Check exact or fuzzy match against targets
            for target in list(TARGET_FILENAMES - set(found.keys())):
                if normalize(target) == normalize(clean_name):
                    found[target] = 'https://sfdpa.nextrequest.com' + doc_path
                    new_hits += 1
                    break
        print(f'({new_hits} new target hits)')
        time.sleep(1)
        if len(found) == len(TARGET_FILENAMES):
            break
    except Exception as e:
        print(f'ERR: {e}')
        break

print(f'\n{len(found)}/{len(TARGET_FILENAMES)} targets resolved')
print()
print('=' * 90)
print('RESOLVED FILES (by case)')
print('=' * 90)
from collections import defaultdict
by_case = defaultdict(list)
for fn, url in found.items():
    m = re.match(r'(?:Production - )?(\d{3,5}-\d{2,4})', fn)
    case = m.group(1) if m else '?'
    by_case[case].append((fn, url))

for case in sorted(by_case.keys()):
    files = by_case[case]
    print(f'\n── CASE {case} ──')
    for fn, url in sorted(files):
        print(f'  {fn[:70]}')
        print(f'    {url}/download')

# Save for later use
with open('gold_case_urls.json', 'w', encoding='utf-8') as f:
    json.dump({fn: url + '/download' for fn, url in found.items()}, f, indent=2)
print(f'\nSaved to gold_case_urls.json')

print('\nMISSING:')
for t in sorted(TARGET_FILENAMES - set(found.keys())):
    print(f'  {t}')
