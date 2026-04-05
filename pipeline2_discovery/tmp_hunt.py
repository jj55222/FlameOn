"""Hunt v4: scrape HTML format to extract proper file→URL mappings."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
import time
from collections import defaultdict, Counter

load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

PRIORITY_REQUESTS = ['22-7', '20-2', '22-9', '22-6', '22-10', '22-8', '20-3', '20-5']

all_files = []

for req_id in PRIORITY_REQUESTS:
    url = f'https://sfdpa.nextrequest.com/requests/{req_id}'
    print(f'\n=== {req_id} ===')
    try:
        result = app.scrape(url, formats=['html'])
        html = getattr(result, 'html', '') or ''
        print(f'  html: {len(html)} chars')

        # Match: <a href="/documents/12345">filename.mp4</a>
        link_pattern = re.compile(
            r'<a[^>]*href="(/documents/\d+)"[^>]*>([^<]+\.(?:mp3|mp4|mov|avi|wav|pdf|docx?|m4a))</a>',
            re.IGNORECASE,
        )
        matches = link_pattern.findall(html)
        print(f'  files: {len(matches)}')

        for doc_path, filename in matches:
            doc_url = 'https://sfdpa.nextrequest.com' + doc_path
            ext = filename.lower().rsplit('.', 1)[-1]
            if ext in ('mp3', 'wav', 'm4a'):
                ftype = 'audio'
            elif ext in ('mp4', 'mov', 'avi'):
                ftype = 'video'
            elif ext in ('pdf', 'docx', 'doc'):
                ftype = 'document'
            else:
                ftype = 'other'
            # Extract case folder from filename
            case_match = re.match(r'[\s]*(\d{3,5}-\d{2,4})', filename)
            folder = case_match.group(1) if case_match else ''
            all_files.append({
                'filename': filename.strip(),
                'doc_url': doc_url,
                'download_url': doc_url + '/download',
                'parent_request': req_id,
                'folder': folder,
                'file_type': ftype,
            })
        time.sleep(1)
    except Exception as e:
        print(f'  ERR: {e}')

# Dedupe
seen = set()
unique = [f for f in all_files if not (f['doc_url'] in seen or seen.add(f['doc_url']))]
print(f'\nTotal unique files: {len(unique)}')

with open('sfdpa_gold_hunt.json', 'w', encoding='utf-8') as f:
    json.dump(unique, f, indent=2, ensure_ascii=False)

# Group by case folder
by_folder = defaultdict(list)
for f in unique:
    by_folder[f['folder'] or '(none)'].append(f)

ranked = []
for folder, files in by_folder.items():
    if folder == '(none)':
        continue
    types = Counter(f['file_type'] for f in files)
    v = types.get('video', 0)
    a = types.get('audio', 0)
    d = types.get('document', 0)
    full_pkg = 50 if (v >= 1 and a >= 1) else 0
    multi_v = 20 if v >= 2 else 0
    multi_a = 10 if a >= 2 else 0
    score = v * 10 + a * 5 + d + full_pkg + multi_v + multi_a
    ranked.append({'score': score, 'folder': folder, 'v': v, 'a': a, 'd': d, 'files': files})
ranked.sort(key=lambda x: -x['score'])

print(f'\n{"=" * 95}')
print(f'{"RANK":>4} {"SCORE":>6} {"FOLDER":>15} {"V":>3} {"A":>3} {"D":>3}  TOTAL')
print('=' * 95)
for i, c in enumerate(ranked[:20], 1):
    print(f'{i:>4} {c["score"]:>6.1f} {c["folder"]:>15s} {c["v"]:>3d} {c["a"]:>3d} {c["d"]:>3d}  {c["v"]+c["a"]+c["d"]:>3d}')

gold = [c for c in ranked if c['v'] >= 1 and c['a'] >= 1]
print(f'\n{"=" * 95}')
print(f'GOLD CANDIDATES — {len(gold)} cases with BOTH video AND audio')
print('=' * 95)
for c in gold[:15]:
    print(f'\n── CASE {c["folder"]} — {c["v"]}V {c["a"]}A {c["d"]}D ──')
    order = {'video': 0, 'audio': 1, 'document': 2}
    c['files'].sort(key=lambda x: (order.get(x['file_type'], 9), x['filename']))
    for f in c['files']:
        k = {'video': 'V', 'audio': 'A', 'document': 'D'}.get(f['file_type'], '?')
        print(f'   [{k}] {f["filename"][:78]}')
        print(f'       {f["download_url"]}')
