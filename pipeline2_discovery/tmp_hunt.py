"""
Hunt v3: use scroll actions + wait_for to fully load the document list on
SF DPA request pages before scraping. Each SF DPA request (20-1 through 22-11)
aggregates all files for its SB1421/SB16 cases.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
import time
from collections import defaultdict, Counter

load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Priority requests — 22-7 (SB16 use-of-force) is the most video-rich
# based on our earlier preview. 20-2 had surveillance + BWC mentions.
PRIORITY_REQUESTS = ['22-7', '20-2', '22-9', '22-6', '22-10']

all_files = []

for req_id in PRIORITY_REQUESTS:
    url = f'https://sfdpa.nextrequest.com/requests/{req_id}'
    print(f'\n=== {req_id} ===')
    try:
        # Use scroll actions to trigger AJAX "show more" loading
        result = app.scrape(
            url,
            formats=['markdown'],
            wait_for=5000,
            actions=[
                {'type': 'scroll', 'direction': 'down'},
                {'type': 'wait', 'milliseconds': 2000},
                {'type': 'scroll', 'direction': 'down'},
                {'type': 'wait', 'milliseconds': 2000},
                {'type': 'scroll', 'direction': 'down'},
                {'type': 'wait', 'milliseconds': 2000},
                {'type': 'scroll', 'direction': 'down'},
                {'type': 'wait', 'milliseconds': 2000},
                {'type': 'scroll', 'direction': 'down'},
                {'type': 'wait', 'milliseconds': 3000},
            ],
        )
        md = getattr(result, 'markdown', '') or ''
        print(f'  md: {len(md)} chars')

        # Parse every file link
        pattern = re.compile(
            r'\[([^\]]+\.(?:mp3|mp4|mov|avi|wav|pdf|docx?|m4a))\]\((https://sfdpa\.nextrequest\.com/documents/\d+)\)',
            re.IGNORECASE,
        )
        matches = pattern.findall(md)
        print(f'  files: {len(matches)}')

        for filename, doc_url in matches:
            ext = filename.lower().rsplit('.', 1)[-1]
            if ext in ('mp3', 'wav', 'm4a'):
                ftype = 'audio'
            elif ext in ('mp4', 'mov', 'avi'):
                ftype = 'video'
            elif ext in ('pdf', 'docx', 'doc'):
                ftype = 'document'
            else:
                ftype = 'other'
            case_match = re.match(r'([\w]*?(\d{3,5}-\d{2,4}))', filename)
            folder = case_match.group(2) if case_match else ''
            all_files.append({
                'filename': filename,
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
print(f'\n{"=" * 90}')
print(f'Total unique files across requests: {len(unique)}')

with open('sfdpa_full_hunt.json', 'w', encoding='utf-8') as f:
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
    o = types.get('other', 0)
    full_pkg = 50 if (v >= 1 and a >= 1) else 0
    multi_v = 20 if v >= 2 else 0
    multi_a = 10 if a >= 2 else 0
    score = v * 10 + a * 5 + d + o * 0.5 + full_pkg + multi_v + multi_a
    if score >= 5:
        ranked.append({
            'score': score, 'folder': folder,
            'v': v, 'a': a, 'd': d, 'o': o,
            'files': files,
        })
ranked.sort(key=lambda x: -x['score'])

print(f'\n{"RANK":>4} {"SCORE":>6} {"FOLDER":>15} {"V":>3} {"A":>3} {"D":>3} {"O":>3}')
print('-' * 50)
for i, c in enumerate(ranked[:20], 1):
    print(f'{i:>4} {c["score"]:>6.1f} {c["folder"]:>15s} {c["v"]:>3d} {c["a"]:>3d} {c["d"]:>3d} {c["o"]:>3d}')

# Gold candidates
gold = [c for c in ranked if c['v'] >= 1 and c['a'] >= 1]
print(f'\n{"=" * 90}')
print(f'GOLD (video AND audio) — {len(gold)} cases')
print('=' * 90)
for c in gold[:10]:
    print(f'\n── CASE {c["folder"]} — {c["v"]}V {c["a"]}A {c["d"]}D {c["o"]}O ──')
    order = {'video': 0, 'audio': 1, 'document': 2, 'other': 3}
    c['files'].sort(key=lambda x: (order.get(x['file_type'], 9), x['filename']))
    for f in c['files']:
        k = {'video': 'V', 'audio': 'A', 'document': 'D', 'other': 'O'}.get(f['file_type'], '?')
        print(f'   [{k}] {f["filename"][:78]}')
