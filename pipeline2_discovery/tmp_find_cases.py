"""Find full-package cases by scraping ALL pages of SF DPA /documents index."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
from collections import defaultdict
import time

load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# SF DPA has 589 total records — with 50/page that's ~12 pages
# We already have 200 in cache; let's pull pages 5-15 to fill in
all_docs = []
import json

try:
    with open('foia_docs_cache.json', encoding='utf-8') as f:
        existing = json.load(f)
    all_docs.extend(existing)
    print(f'Loaded {len(existing)} existing docs from cache')
except FileNotFoundError:
    pass

# Pull pages 5-15
for p in range(5, 16):
    url = f'https://sfdpa.nextrequest.com/documents?page={p}'
    print(f'  Page {p}...', end=' ')
    try:
        result = app.scrape(url, formats=['markdown'])
        md = getattr(result, 'markdown', '') or ''

        row_pattern = re.compile(
            r'\[([^\]]+)\]\((https://[^)]*/documents/\d+)\)\s*\|\s*'
            r'\[([\w\-]+)\]\((https://[^)]*/requests/[\w\-]+)\)\s*\|\s*'
            r'([\d/]+)\s*\|\s*'
            r'(\d+)\s*\|\s*'
            r'([^|]*)\|\s*'
            r'([^|]*)\|\s*'
            r'([^|\n]*)'
        )
        count = 0
        for m in row_pattern.finditer(md):
            filename = m.group(1).strip()
            doc_url = m.group(2).strip()
            req_id = m.group(3).strip()
            folder = m.group(7).strip()
            ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
            if ext in ('mp3', 'wav', 'm4a', 'aac'):
                ftype = 'audio'
            elif ext in ('mp4', 'mov', 'avi', 'mkv', 'webm'):
                ftype = 'video'
            elif ext in ('pdf', 'doc', 'docx'):
                ftype = 'document'
            else:
                ftype = 'other'
            all_docs.append({
                'filename': filename,
                'doc_url': doc_url,
                'download_url': doc_url.rstrip('/') + '/download',
                'request_id': req_id,
                'folder': folder,
                'file_type': ftype,
                'extension': ext,
                'portal_key': 'San Francisco DPA',
            })
            count += 1
        print(f'+{count} docs')
        if count == 0:
            break
        time.sleep(1)
    except Exception as e:
        print(f'ERR: {e}')
        break

# Dedupe by doc_url
seen = set()
unique_docs = []
for d in all_docs:
    if d['doc_url'] not in seen:
        seen.add(d['doc_url'])
        unique_docs.append(d)

print(f'\nTotal unique docs: {len(unique_docs)}')

with open('foia_docs_cache.json', 'w', encoding='utf-8') as f:
    json.dump(unique_docs, f, indent=2, ensure_ascii=False)

# Now group by folder and rank
by_folder = defaultdict(list)
for d in unique_docs:
    folder = d.get('folder', '').strip() or '(no folder)'
    by_folder[folder].append(d)

ranked = []
for folder, fdocs in by_folder.items():
    if folder == '(no folder)':
        continue
    vids = sum(1 for d in fdocs if d['file_type'] == 'video')
    auds = sum(1 for d in fdocs if d['file_type'] == 'audio')
    docs = sum(1 for d in fdocs if d['file_type'] == 'document')
    other = sum(1 for d in fdocs if d['file_type'] == 'other')
    # Score: video is king (multi-angle BWC), audio second
    score = vids * 10 + auds * 3 + docs + other * 0.5
    ranked.append((score, folder, vids, auds, docs, other, fdocs))

ranked.sort(reverse=True)

print('\n' + '=' * 95)
print(f'{"SCORE":>7} {"FOLDER":25s} {"VID":>4} {"AUD":>4} {"DOC":>4} {"OTH":>4}  {"TOTAL":>6}')
print('=' * 95)
for score, folder, vids, auds, docs, other, fdocs in ranked[:20]:
    print(f'{score:7.1f} {folder[:25]:25s} {vids:>4d} {auds:>4d} {docs:>4d} {other:>4d}  {len(fdocs):>6d}')

# Show full manifest of top 3 cases with video
print('\n' + '=' * 95)
print('TOP 5 VIDEO-HAVING CASES — FULL MANIFEST')
print('=' * 95)
video_cases = [r for r in ranked if r[2] > 0][:5]
for score, folder, vids, auds, docs, other, fdocs in video_cases:
    print(f'\n{"─" * 80}')
    print(f'CASE {folder} ─ {vids}V {auds}A {docs}D {other}O ─ score {score}')
    print("─" * 80)
    # Dedupe by filename
    seen_fn = set()
    unique = [d for d in fdocs if not (d['filename'] in seen_fn or seen_fn.add(d['filename']))]
    order_map = {'video': 0, 'audio': 1, 'document': 2, 'other': 3}
    unique.sort(key=lambda x: (order_map.get(x['file_type'], 9), x['filename']))
    for d in unique:
        k = {'video': 'V', 'audio': 'A', 'document': 'D', 'other': 'O'}.get(d['file_type'], '?')
        print(f'  [{k}] {d["filename"][:78]}')
