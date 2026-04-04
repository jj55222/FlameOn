"""Temporary helper — find full-package cases in SF DPA request 22-7."""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
from collections import defaultdict

load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

url = 'https://sfdpa.nextrequest.com/requests/22-7'
result = app.scrape(url, formats=['markdown'])
md = getattr(result, 'markdown', '') or ''
print(f'md length: {len(md)}')

doc_pattern = re.compile(r'\[([^\]]+)\]\((https://sfdpa\.nextrequest\.com/documents/\d+)\)')
matches = doc_pattern.findall(md)
print(f'Total doc matches: {len(matches)}')

# Group by case ID extracted from filename
by_case = defaultdict(list)
for filename, doc_url in matches:
    case_match = re.match(r'(\d{3,5}-\d{2,4})', filename)
    if case_match:
        case_id = case_match.group(1)
        by_case[case_id].append((filename, doc_url))

# Rank cases by: (videos * 10) + (audios * 3) + docs
ranked = []
for case_id, files in by_case.items():
    seen = set()
    unique = [(f, u) for f, u in files if not (f in seen or seen.add(f))]
    vids = sum(1 for f, _ in unique if f.lower().endswith(('.mp4', '.mov', '.avi')))
    auds = sum(1 for f, _ in unique if f.lower().endswith('.mp3'))
    docs = sum(1 for f, _ in unique if f.lower().endswith('.pdf'))
    if vids + auds == 0:
        continue  # skip doc-only cases
    score = vids * 10 + auds * 3 + docs
    ranked.append((score, case_id, vids, auds, docs, unique))

ranked.sort(reverse=True)

print('\n' + '=' * 90)
print(f'{"SCORE":>6} {"CASE":>12} {"VID":>4} {"AUD":>4} {"DOC":>4}')
print('=' * 90)
for score, case_id, vids, auds, docs, _ in ranked[:15]:
    print(f'{score:6d} {case_id:>12s} {vids:>4d} {auds:>4d} {docs:>4d}')

# Show top 3 in detail
print('\n' + '=' * 90)
print('TOP 3 CASES — FULL MANIFEST')
print('=' * 90)
for score, case_id, vids, auds, docs, unique in ranked[:3]:
    print(f'\n--- CASE {case_id} ({vids}V {auds}A {docs}D) ---')
    # Sort: videos first, audios second, then docs
    order = {'.mp4': 0, '.mov': 0, '.mp3': 1, '.pdf': 2}

    def sort_key(x):
        fn = x[0].lower()
        for ext, o in order.items():
            if fn.endswith(ext):
                return (o, fn)
        return (9, fn)

    unique.sort(key=sort_key)
    for fn, du in unique:
        ext = fn.lower().rsplit('.', 1)[-1] if '.' in fn else ''
        kind = 'V' if ext in ('mp4', 'mov', 'avi') else ('A' if ext == 'mp3' else 'D')
        print(f'  [{kind}] {fn[:78]}')
        print(f'      {du}/download')
