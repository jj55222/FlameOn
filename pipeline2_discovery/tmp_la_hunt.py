"""Pull titles + metadata for all Mesa PD CIB videos via yt-dlp."""
import yt_dlp
import re
import json

VIDEO_IDS = [
    'vaYNbMGnHaw', 'Qynv4kHuOng', '80sXQHkCJlg', 'x5S2o4UPIdg', 'cbKB_vyVlyA',
    'hAjCBowSPMI', 'NO5FdppAB9Y', 'PEkV2xR_Jr0', 'gqd5CeSsmsg', 'IncDC3QcJOM',
    'exiUK6XijfY', 'wOfO9pAPnzw', 'U22VOe-ztt8', 'v69V7UEhczc', 'aGKcCTxtqAg',
    's1bS2vwB1d0', 'H5QJPcWZz14', 'zT6GTxGhyKk', '3t6fe7CbE7Q', 'fWYiwSwS1zg',
    'MC1vI6GhOdU', 'bfSm6Fi1dAQ', 'd86D7ufDUY0', 'I_3R3_V1EDM', 'chUD541b9pE',
    'CqI3Ow43IYA', 'brFnTfOM1YE', 'mAaVWXuaK_I', 'hbJYag8BJbM', 'mOVkBEKFhAs',
    't8Kx1AH6Vbk', 'wkrGNVM69N4', 'Ki7LK3FEnVI', 'TUgFFTMmtnI', 'sEoOqlM-yTA',
    'DD-d0lv0QjE', 'weMGaoXV5zs', 'HLc7FfvHgEY', 'oECRvOn-p4s', 'SCc5e7ISA_4',
    '7Hh39StlI_4',
]

opts = {'quiet': True, 'no_warnings': True, 'skip_download': True}
cases = []
for vid in VIDEO_IDS:
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f'https://www.youtube.com/watch?v={vid}', download=False)
            cases.append({
                'video_id': vid,
                'title': info.get('title', ''),
                'duration': info.get('duration', 0),
                'upload_date': info.get('upload_date', ''),
                'view_count': info.get('view_count', 0),
                'description': (info.get('description', '') or '')[:500],
            })
            print(f'{info.get("duration", 0):>6}s  {info.get("title", "")[:80]}')
    except Exception as e:
        print(f'ERR {vid}: {str(e)[:60]}')

# Sort by duration (longer = more content)
cases.sort(key=lambda x: -x['duration'])

print()
print('=' * 90)
print('MESA PD CIB VIDEOS RANKED BY DURATION (non-protest crime cases)')
print('=' * 90)
# Filter out protest/related and any "explanation of tools" videos
EXCLUDE = ['protest', 'demonstration', 'less lethal', 'beanbag', 'pepper', 'taser', 'tool', 'bolo', 'crowd control', 'chemical']
for c in cases:
    title_lower = c['title'].lower()
    if any(kw in title_lower for kw in EXCLUDE):
        continue
    mins = c['duration'] // 60
    secs = c['duration'] % 60
    date_fmt = c['upload_date'][:4] + '-' + c['upload_date'][4:6] + '-' + c['upload_date'][6:] if c['upload_date'] else '?'
    print(f'  [{mins:>3}:{secs:02d}]  {date_fmt}  {c["view_count"]:>8,} views  {c["title"][:75]}')
    print(f'          https://www.youtube.com/watch?v={c["video_id"]}')

with open('mesa_cases.json', 'w', encoding='utf-8') as f:
    json.dump(cases, f, indent=2, ensure_ascii=False)
