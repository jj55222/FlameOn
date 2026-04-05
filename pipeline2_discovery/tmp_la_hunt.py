"""Get full descriptions for top-view Mesa PD OIS videos."""
import yt_dlp

TOP = [
    ('SCc5e7ISA_4', 'Robson, OIS, July 7, 2022',          47927, 252),
    ('bfSm6Fi1dAQ', 'Standage, OIS, March 15, 2023',      42274, 403),
    ('DD-d0lv0QjE', '9th Street, OIS, August 22, 2022',   30118, 411),
    ('gqd5CeSsmsg', 'Main St OIS February 18, 2022',      22729, 333),
    ('wkrGNVM69N4', 'OIS_19th & Southern_Sept 18, 2020',  22451, 520),
    ('I_3R3_V1EDM', 'Alma School/US 60',                  19563, 366),
    ('Qynv4kHuOng', '350 E 5th Avenue, OIS, July 11 2025', 18878, 371),
    ('t8Kx1AH6Vbk', 'OIS MCKELLIPS 01 02 22',             18363, 498),
    ('v69V7UEhczc', 'Gilbert Rd OIS, May 9, 2022',        18733, 390),
    ('vaYNbMGnHaw', 'NMesa Drive, OIS, July 6, 2023',     17101, 509),
]

opts = {'quiet': True, 'no_warnings': True, 'skip_download': True}
for vid_id, label, views, dur in TOP:
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f'https://www.youtube.com/watch?v={vid_id}', download=False)
            desc = info.get('description', '') or ''
            print(f'\n{"="*80}')
            print(f'{label}')
            print(f'  {views:,} views | {dur}s ({dur//60}:{dur%60:02d})')
            print(f'  https://www.youtube.com/watch?v={vid_id}')
            print(f'{"="*80}')
            print(desc[:1800])
    except Exception as e:
        print(f'ERR {vid_id}: {str(e)[:80]}')
