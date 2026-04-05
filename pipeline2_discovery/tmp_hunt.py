"""
Check if filenames listed on request pages can be resolved to doc IDs
via the global /documents search, OR if a download-all button exists.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Get HTML of request 22-7 and extract all filenames mentioned
print('Step 1: Extract all filenames from request 22-7')
result = app.scrape('https://sfdpa.nextrequest.com/requests/22-7', formats=['html'])
html = getattr(result, 'html', '') or ''

# All filenames inside <p class="wrap-text quill-content">
paragraphs = re.findall(r'<p class="wrap-text quill-content">(.*?)</p>', html, re.DOTALL)
all_filenames = set()
for p in paragraphs:
    # Split on <br> and strip tags
    lines = re.split(r'<br\s*/?>', p)
    for line in lines:
        clean = re.sub(r'<[^>]+>', '', line).strip()
        if clean and '.' in clean:
            all_filenames.add(clean)

print(f'  Found {len(all_filenames)} unique filenames referenced:')
for f in sorted(all_filenames):
    print(f'    {f[:80]}')

# Group by case folder
from collections import defaultdict, Counter
by_case = defaultdict(list)
for f in all_filenames:
    case_m = re.match(r'(\d{3,5}-\d{2,4})', f)
    if not case_m and f.lower().startswith('production'):
        case_m = re.search(r'(\d{3,5}-\d{2,4})', f)
    folder = case_m.group(1) if case_m else ''
    by_case[folder].append(f)

print(f'\nStep 2: Rank cases by content variety')
print('-' * 80)
ranked = []
for folder, files in by_case.items():
    if not folder:
        continue
    types = {'video': 0, 'audio': 0, 'document': 0}
    for f in files:
        ext = f.lower().rsplit('.', 1)[-1] if '.' in f else ''
        if ext in ('mp4', 'mov', 'avi'):
            types['video'] += 1
        elif ext in ('mp3', 'wav', 'm4a'):
            types['audio'] += 1
        elif ext in ('pdf', 'docx'):
            types['document'] += 1
    v, a, d = types['video'], types['audio'], types['document']
    full_pkg = 50 if (v >= 1 and a >= 1) else 0
    score = v * 10 + a * 5 + d + full_pkg
    if score >= 5:
        ranked.append({'folder': folder, 'score': score, 'v': v, 'a': a, 'd': d, 'files': files})
ranked.sort(key=lambda x: -x['score'])

for c in ranked[:15]:
    print(f'  {c["folder"]:>12s}  score={c["score"]:>4.0f}  V{c["v"]} A{c["a"]} D{c["d"]}')

# For the top gold candidate, resolve every filename to a doc_url
# by searching /documents with the filename as a filter
print(f'\nStep 3: Resolve doc URLs for top gold cases via /documents search')
gold = [c for c in ranked if c['v'] >= 1 and c['a'] >= 1]
print(f'  Gold candidates: {len(gold)}')

if gold:
    # We already have foia_docs_cache.json with 200 docs from earlier scraping
    try:
        with open('foia_docs_cache.json', encoding='utf-8') as f:
            cache = json.load(f)
    except FileNotFoundError:
        cache = []

    cache_by_filename = {d['filename']: d for d in cache}
    print(f'  Cache has {len(cache_by_filename)} known filenames')

    for c in gold[:5]:
        print(f'\n  === CASE {c["folder"]} ===')
        for fn in c['files']:
            resolved = cache_by_filename.get(fn)
            if resolved:
                print(f'    [✓] {fn[:60]}')
                print(f'        {resolved["download_url"]}')
            else:
                print(f'    [?] {fn[:60]} (not in cache)')
