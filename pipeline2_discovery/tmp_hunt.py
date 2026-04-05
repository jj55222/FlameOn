"""
Option C: single Firecrawl extract call on request 22-7 to get all
filename -> document URL mappings. Costs ~21 credits but gives us
everything in one shot.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import json
import time
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

url = 'https://sfdpa.nextrequest.com/requests/22-7'
print(f'Extracting files from {url}...')
start = time.time()

schema = {
    'type': 'object',
    'properties': {
        'files': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'filename': {'type': 'string'},
                    'document_url': {'type': 'string', 'description': 'The /documents/NNNN URL that clicking this file links to'},
                    'case_folder': {'type': 'string', 'description': 'Case folder ID like 0213-18 extracted from filename'},
                    'file_type': {'type': 'string', 'enum': ['audio', 'video', 'document', 'other']},
                },
            },
        },
    },
}

try:
    result = app.extract(
        urls=[url],
        prompt='''Extract every file listed in the Documents timeline of this NextRequest page.
For each file provide:
- filename (exact name like "0213-18 - BWC of Lt. Christopher Wilhelm #1282.mp4")
- document_url (the href URL that clicking this filename opens, should be of the form https://sfdpa.nextrequest.com/documents/NNNNNN)
- case_folder (extract the case ID at the start of filename, e.g., "0213-18")
- file_type ("video" for .mp4/.mov, "audio" for .mp3/.wav, "document" for .pdf/.docx, "other" otherwise)

Return ALL files visible on the page, even if 20+ files. Check the click handlers and href attributes carefully.''',
        schema=schema,
        timeout=120,
    )
    elapsed = time.time() - start

    data = result.data if hasattr(result, 'data') else {}
    files = data.get('files', []) if isinstance(data, dict) else []
    credits = getattr(result, 'credits_used', '?')

    print(f'Elapsed: {elapsed:.1f}s, credits: {credits}')
    print(f'Files extracted: {len(files)}')

    # Check how many have valid document_urls
    with_url = [f for f in files if f.get('document_url') and '/documents/' in f.get('document_url', '')]
    print(f'Files WITH valid document_url: {len(with_url)}')

    # Save
    with open('gold_case_urls.json', 'w', encoding='utf-8') as f:
        json.dump(files, f, indent=2, ensure_ascii=False)

    # Group by case
    from collections import defaultdict, Counter
    by_case = defaultdict(list)
    for f in files:
        by_case[f.get('case_folder', '?')].append(f)

    print()
    print('=' * 90)
    print('RESULTS BY CASE')
    print('=' * 90)
    for case, flist in sorted(by_case.items()):
        types = Counter(f.get('file_type', '?') for f in flist)
        with_urls = sum(1 for f in flist if f.get('document_url') and '/documents/' in f.get('document_url', ''))
        print(f'\n── {case} — {len(flist)} files ({dict(types)}, {with_urls} with URLs) ──')
        for f in flist:
            has_url = '✓' if f.get('document_url') and '/documents/' in f.get('document_url', '') else '✗'
            ft = f.get('file_type', '?')[:3]
            print(f'  [{has_url}] [{ft}] {f.get("filename", "?")[:70]}')
            if f.get('document_url'):
                print(f'         {f["document_url"]}')

except Exception as e:
    print(f'EXTRACT FAILED: {e}')

# Check Firecrawl quota
with open('firecrawl_quota.json') as f:
    q = json.load(f)
print(f'\nFirecrawl quota now: {q}')
