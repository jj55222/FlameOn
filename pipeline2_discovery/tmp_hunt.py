"""
Hunt strategy v2: probe SF DPA request pages directly.
The /documents global index only surfaces ~200 records via pagination,
but the parent requests (20-2, 20-3, 22-7, 22-9, etc.) list ALL their
attached files via timeline entries. Scrape each request page to find
video+audio full-package cases.
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

# SF DPA parent requests that aggregate SB1421/SB16 cases
# 20-1 through 20-6: original SB1421 disclosures
# 22-1 through 22-11: SB16 (2022 expansion)
DPA_REQUESTS = [
    '20-1', '20-2', '20-3', '20-4', '20-5', '20-6',
    '22-1', '22-2', '22-3', '22-4', '22-5', '22-6', '22-7', '22-8', '22-9', '22-10', '22-11',
]

all_files = []

for req_id in DPA_REQUESTS:
    url = f'https://sfdpa.nextrequest.com/requests/{req_id}'
    print(f'  {req_id}...', end=' ', flush=True)
    try:
        result = app.scrape(url, formats=['markdown'])
        md = getattr(result, 'markdown', '') or ''

        # Find all [filename](doc_url) within the request page
        # These are the timeline entries with attached documents
        file_pattern = re.compile(r'\[([^\]]+\.(?:mp3|mp4|mov|avi|wav|pdf|docx?))\]\((https://sfdpa\.nextrequest\.com/documents/\d+)\)', re.IGNORECASE)
        matches = file_pattern.findall(md)

        print(f'+{len(matches)} files')
        for filename, doc_url in matches:
            ext = filename.lower().rsplit('.', 1)[-1]
            if ext in ('mp3', 'wav', 'm4a'):
                ftype = 'audio'
            elif ext in ('mp4', 'mov', 'avi', 'wmv'):
                ftype = 'video'
            elif ext in ('pdf', 'doc', 'docx'):
                ftype = 'document'
            else:
                ftype = 'other'
            # Extract case folder from filename (format: NNNN-NN)
            case_match = re.match(r'([\w\-]*\d{3,5}-\d{2,4})', filename)
            folder = case_match.group(1).strip() if case_match else ''
            all_files.append({
                'filename': filename,
                'doc_url': doc_url,
                'download_url': doc_url + '/download',
                'parent_request': req_id,
                'parent_url': url,
                'folder': folder,
                'file_type': ftype,
                'ext': ext,
            })
        time.sleep(1)
    except Exception as e:
        print(f'ERR: {str(e)[:60]}')

# Dedupe by doc_url
seen = set()
unique = [f for f in all_files if not (f['doc_url'] in seen or seen.add(f['doc_url']))]
print(f'\nTotal unique files: {len(unique)}')

with open('sfdpa_full_hunt.json', 'w', encoding='utf-8') as f:
    json.dump(unique, f, indent=2, ensure_ascii=False)

# Group by folder (case)
by_folder = defaultdict(list)
for f in unique:
    key = f['folder'] or '(no folder)'
    by_folder[key].append(f)

# Rank
ranked = []
for folder, files in by_folder.items():
    if folder == '(no folder)':
        continue
    types = Counter(f['file_type'] for f in files)
    vids = types.get('video', 0)
    auds = types.get('audio', 0)
    docs = types.get('document', 0)
    if vids + auds == 0 and docs < 2:
        continue
    # Gold scoring: video + audio = full package
    full_pkg_bonus = 50 if (vids >= 1 and auds >= 1) else 0
    multi_angle = 20 if vids >= 2 else 0
    multi_interview = 10 if auds >= 2 else 0
    score = vids * 10 + auds * 5 + docs * 1 + full_pkg_bonus + multi_angle + multi_interview
    ranked.append({
        'score': score, 'folder': folder,
        'vids': vids, 'auds': auds, 'docs': docs,
        'files': files,
    })
ranked.sort(key=lambda x: -x['score'])

print('\n' + '=' * 95)
print(f'{"RANK":>4} {"SCORE":>6} {"FOLDER":>20} {"VID":>4} {"AUD":>4} {"DOC":>4}')
print('=' * 95)
for i, c in enumerate(ranked[:20], 1):
    print(f'{i:>4} {c["score"]:>6.1f} {c["folder"]:>20s} {c["vids"]:>4d} {c["auds"]:>4d} {c["docs"]:>4d}')

# Detailed manifest of gold
gold = [c for c in ranked if c['vids'] >= 1 and c['auds'] >= 1]
print(f'\n{"=" * 95}')
print(f'GOLD CANDIDATES — {len(gold)} cases with BOTH video and audio')
print('=' * 95)
for c in gold[:10]:
    print(f'\n── CASE {c["folder"]} — {c["vids"]}V {c["auds"]}A {c["docs"]}D ──')
    order = {'video': 0, 'audio': 1, 'document': 2}
    c['files'].sort(key=lambda x: (order.get(x['file_type'], 9), x['filename']))
    for f in c['files']:
        k = {'video': 'V', 'audio': 'A', 'document': 'D'}.get(f['file_type'], '?')
        print(f'   [{k}] {f["filename"][:78]}')
        print(f'       {f["download_url"]}')
