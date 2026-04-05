"""Debug: why is the request page returning less content now?"""
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import os
import re
load_dotenv()
app = FirecrawlApp(api_key=os.environ['FIRECRAWL_API_KEY'])

# Plain scrape, no actions
url = 'https://sfdpa.nextrequest.com/requests/22-7'
print(f'Scraping {url} (plain)...')
result = app.scrape(url, formats=['markdown'], max_age=0)
md = getattr(result, 'markdown', '') or ''
print(f'md length: {len(md)}')
print('=' * 80)
print(md)
