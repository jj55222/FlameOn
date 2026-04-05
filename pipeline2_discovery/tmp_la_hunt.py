"""
Try multiple proactive BWC agency portals that we haven't tested yet.
Goal: find one with a list of CRIMINAL incident briefings (not protests)
with direct video links.
"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
import json
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])


def probe(name, url):
    print(f'\n{"=" * 80}\n{name}\n{url}\n{"=" * 80}')
    try:
        result = app.scrape(url, formats=['markdown', 'links'])
        md = getattr(result, 'markdown', '') or ''
        links = getattr(result, 'links', []) or []
        print(f'md: {len(md)} chars, links: {len(links)}')

        # Find case-specific links (incident, shooting, arrest, murder, etc.)
        case_links = set()
        for line in md.split('\n'):
            if any(kw in line.lower() for kw in ['incident', 'shooting', 'arrest', 'suspect', 'case', 'briefing', 'critical']):
                urls = re.findall(r'https?://[^\s\)\]\">]+', line)
                case_links.update(urls)

        # Filter out generic nav
        case_links = {u for u in case_links if not any(s in u.lower() for s in
                      ['#', 'facebook', 'twitter', 'addtoany', 'mailto:', '.css', '.js', 'login'])}

        print(f'\nCase-related URLs: {len(case_links)}')
        for u in sorted(case_links)[:30]:
            print(f'  {u}')

        # Also find Vimeo/YouTube direct video links
        videos = set(re.findall(r'https?://(?:www\.)?(?:vimeo\.com|youtube\.com|youtu\.be)/[^\s\)\]\">]+', md))
        if videos:
            print(f'\nDirect video links: {len(videos)}')
            for v in videos:
                print(f'  VIDEO: {v}')
    except Exception as e:
        print(f'ERR: {str(e)[:100]}')


# List of target proactive portals
portals = [
    ('Phoenix Critical Incident Briefings', 'https://www.phoenix.gov/newsroom/police-department-news/critical-incident-briefings.html'),
    ('Mesa Community Briefings (listing)',  'https://www.mesaaz.gov/Public-Safety/Mesa-Police/Community/Transparency-In-Policing/Community-Briefings'),
    ('Las Vegas Metro PD Use of Force',     'https://www.lvmpd.com/transparency/officer-involved-shootings'),
    ('Austin PD Critical Incident videos',  'https://www.austintexas.gov/department/austin-police-critical-incident-videos'),
    ('Seattle PD Critical Incident',        'https://www.seattle.gov/police/information-and-data/use-of-force-data/critical-incident-videos'),
    ('Houston PD OIS',                      'https://www.houstontx.gov/police/ois/'),
]

for name, url in portals:
    probe(name, url)
