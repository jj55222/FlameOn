"""
Hunt for full-package cases in LA City + SF citywide NextRequest portals.
Strategy: scrape multiple pages of /documents, group by request_id, rank by
content variety (video + audio + docs = full package).
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
    """Parse NextRequest /documents markdown table into structured records."""
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
        upload = m.group(5).strip()
        downloads = int(m.group(6).strip())
        folder = m.group(7).strip()
        ext = filename.lower().rsplit('.', 1)[-1] if '.' in filename else ''
        if ext in ('mp3', 'wav', 'm4a', 'aac'):
            ftype = 'audio'
        elif ext in ('mp4', 'mov', 'avi', 'mkv', 'webm', 'm4v', 'mpg'):
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
            'upload_date': upload,
            'downloads': downloads,
            'folder': folder,
            'file_type': ftype,
            'ext': ext,
        })
    return records


def scrape_portal(name, base_url, max_pages=10):
    """Scrape /documents pages of a NextRequest portal."""
    all_recs = []
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        print(f'  [{name}] page {page}...', end=' ', flush=True)
        try:
            result = app.scrape(url, formats=['markdown'])
            md = getattr(result, 'markdown', '') or ''
            recs = parse_page(md)
            print(f'+{len(recs)}')
            if not recs:
                break
            for r in recs:
                r['portal'] = name
            all_recs.extend(recs)
            time.sleep(1)
        except Exception as e:
            print(f'ERR: {str(e)[:60]}')
            break
    return all_recs


def rank_cases(all_recs):
    """Group by request_id, rank by multi-artifact score."""
    by_req = defaultdict(list)
    for r in all_recs:
        by_req[r['request_id']].append(r)

    ranked = []
    for req_id, recs in by_req.items():
        # Dedupe by filename
        seen = set()
        unique = [r for r in recs if not (r['filename'] in seen or seen.add(r['filename']))]
        types = Counter(r['file_type'] for r in unique)
        vids = types.get('video', 0)
        auds = types.get('audio', 0)
        docs = types.get('document', 0)
        other = types.get('other', 0)
        # Full-package bonus: having BOTH video AND audio is huge
        bonus = 20 if (vids >= 1 and auds >= 1) else 0
        # Multi-angle BWC bonus: 2+ videos
        multi_angle = 10 if vids >= 2 else 0
        score = vids * 8 + auds * 4 + docs * 1 + other * 0.5 + bonus + multi_angle
        if score < 5:
            continue
        ranked.append({
            'score': score,
            'request_id': req_id,
            'request_url': unique[0].get('request_url', ''),
            'portal': unique[0].get('portal', ''),
            'vids': vids, 'auds': auds, 'docs': docs, 'other': other,
            'files': unique,
        })

    ranked.sort(key=lambda x: -x['score'])
    return ranked


def main():
    print('=' * 90)
    print('Hunting for full-package cases in NextRequest portals')
    print('=' * 90)

    all_recs = []

    # LA City — 57K records, known to have video
    la_recs = scrape_portal('LA City', 'https://lacity.nextrequest.com/documents', max_pages=8)
    all_recs.extend(la_recs)

    # SF citywide — 550K records
    sf_recs = scrape_portal('SF citywide', 'https://sanfrancisco.nextrequest.com/documents', max_pages=8)
    all_recs.extend(sf_recs)

    # Save all raw records
    with open('foia_hunt_raw.json', 'w', encoding='utf-8') as f:
        json.dump(all_recs, f, indent=2, ensure_ascii=False)
    print(f'\nTotal raw records: {len(all_recs)}')

    # File type breakdown
    types = Counter(r['file_type'] for r in all_recs)
    print(f'Types: {dict(types)}')

    # Rank cases
    ranked = rank_cases(all_recs)

    print('\n' + '=' * 90)
    print(f'TOP 15 CASES BY CONTENT VARIETY')
    print('=' * 90)
    print(f'{"RANK":>4} {"SCORE":>6} {"REQ ID":>10} {"VID":>4} {"AUD":>4} {"DOC":>4} {"OTH":>4}  PORTAL')
    for i, c in enumerate(ranked[:15], 1):
        print(f'{i:>4} {c["score"]:>6.1f} {c["request_id"]:>10s} {c["vids"]:>4d} {c["auds"]:>4d} {c["docs"]:>4d} {c["other"]:>4d}  {c["portal"]}')
        print(f'     └─ {c["request_url"]}')

    # Show full manifest of top 5 gold candidates (those with both video AND audio)
    gold = [c for c in ranked if c['vids'] >= 1 and c['auds'] >= 1][:5]
    print('\n' + '=' * 90)
    print(f'GOLD CANDIDATES (video AND audio) — {len(gold)} found')
    print('=' * 90)
    for c in gold:
        print(f'\n── REQUEST {c["request_id"]} ({c["portal"]}) — {c["vids"]}V {c["auds"]}A {c["docs"]}D {c["other"]}O ──')
        print(f'   {c["request_url"]}')
        order = {'video': 0, 'audio': 1, 'document': 2, 'other': 3}
        c['files'].sort(key=lambda x: (order.get(x['file_type'], 9), x['filename']))
        for f in c['files']:
            k = {'video': 'V', 'audio': 'A', 'document': 'D', 'other': 'O'}.get(f['file_type'], '?')
            print(f'   [{k}] {f["filename"][:78]}')
            print(f'        {f["download_url"]}')

    # Also show top video-only cases
    video_only = [c for c in ranked if c['vids'] >= 2 and c['auds'] == 0][:5]
    if video_only:
        print('\n' + '=' * 90)
        print(f'MULTI-VIDEO CASES (2+ BWC angles, no audio attached)')
        print('=' * 90)
        for c in video_only:
            print(f'\n── REQUEST {c["request_id"]} ({c["portal"]}) — {c["vids"]}V {c["docs"]}D ──')
            print(f'   {c["request_url"]}')
            for f in c['files']:
                if f['file_type'] in ('video', 'document'):
                    k = {'video': 'V', 'document': 'D'}[f['file_type']]
                    print(f'   [{k}] {f["filename"][:78]}')


if __name__ == '__main__':
    main()
