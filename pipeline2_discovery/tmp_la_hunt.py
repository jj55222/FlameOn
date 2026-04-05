"""
Hunt for criminal cases with BWC in LA City NextRequest.
LA City has 57K records. Strategy: scrape deep pages of /documents,
filter for video files, then group by parent request to find clusters
where a single case has BWC + other content.
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


def parse_page(md):
    """Parse NextRequest /documents markdown table into records."""
    row_pattern = re.compile(
        r'\[([^\]]+)\]\((https://[^)]*/documents/\d+)\)\s*\|\s*'
        r'\[([\w\-]+)\]\((https://[^)]*/requests/[\w\-]+)\)\s*\|\s*'
        r'([\d/]+)\s*\|\s*'
        r'(\d+)\s*\|\s*'
        r'([^|]*)\|\s*'
        r'([^|]*)\|\s*'
        r'([^|\n]*)'
    )
    records = []
    for m in row_pattern.finditer(md):
        filename = m.group(1).strip()
        doc_url = m.group(2).strip()
        req_id = m.group(3).strip()
        req_url = m.group(4).strip()
        folder = m.group(7).strip()
        description = m.group(9).strip()
        ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
        if ext in ('mp3', 'wav', 'm4a', 'aac'):
            ftype = 'audio'
        elif ext in ('mp4', 'mov', 'avi', 'mkv', 'webm', 'mpg', 'm4v', 'wmv'):
            ftype = 'video'
        elif ext in ('pdf', 'doc', 'docx'):
            ftype = 'document'
        else:
            ftype = 'other'
        records.append({
            'filename': filename,
            'doc_url': doc_url,
            'download_url': doc_url + '/download',
            'request_id': req_id,
            'request_url': req_url,
            'folder': folder,
            'description': description,
            'file_type': ftype,
        })
    return records


# Scrape LA City /documents — try ~20 pages to get ~600-900 records
BASE = 'https://lacity.nextrequest.com/documents'
all_recs = []

for page in range(1, 21):
    url = BASE if page == 1 else f"{BASE}?page={page}"
    print(f'  page {page}...', end=' ', flush=True)
    try:
        result = app.scrape(url, formats=['markdown'])
        md = getattr(result, 'markdown', '') or ''
        recs = parse_page(md)
        print(f'+{len(recs)}')
        if not recs:
            break
        all_recs.extend(recs)
        time.sleep(1)
    except Exception as e:
        print(f'ERR: {str(e)[:60]}')
        break

# Dedupe
seen = set()
unique = [r for r in all_recs if not (r['doc_url'] in seen or seen.add(r['doc_url']))]
print(f'\nTotal unique records: {len(unique)}')
print(f'Types: {Counter(r["file_type"] for r in unique)}')

with open('la_city_cache.json', 'w', encoding='utf-8') as f:
    json.dump(unique, f, indent=2, ensure_ascii=False)

# Find video files
videos = [r for r in unique if r['file_type'] == 'video']
print(f'\nVideo files found: {len(videos)}')
for v in videos[:30]:
    print(f'  [{v["request_id"]}] {v["filename"][:75]}')

# Group by request_id to find video-rich cases
by_req = defaultdict(list)
for r in unique:
    by_req[r['request_id']].append(r)

print('\n' + '=' * 90)
print('REQUESTS WITH VIDEO (ranked by content count)')
print('=' * 90)
ranked = []
for req_id, files in by_req.items():
    types = Counter(f['file_type'] for f in files)
    if types.get('video', 0) < 1:
        continue
    ranked.append({
        'req_id': req_id,
        'vids': types.get('video', 0),
        'auds': types.get('audio', 0),
        'docs': types.get('document', 0),
        'other': types.get('other', 0),
        'files': files,
    })

ranked.sort(key=lambda x: -(x['vids'] * 10 + x['auds'] * 3 + x['docs']))
for c in ranked[:20]:
    print(f'  [{c["req_id"]:>10s}]  V{c["vids"]} A{c["auds"]} D{c["docs"]} O{c["other"]}')
    # Show titles
    for f in c['files'][:4]:
        t = f['file_type'][:3]
        print(f'     [{t}] {f["filename"][:72]}')
    if len(c['files']) > 4:
        print(f'     ... and {len(c["files"]) - 4} more')
    print()
