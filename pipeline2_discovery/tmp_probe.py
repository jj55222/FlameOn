"""Download all 5 files for Case 0409-18."""
import requests
import re
import os
import time

CASE_DIR = 'C:/FlameON/FlameOn-main/pipeline3_audio/case_0409-18'
os.makedirs(CASE_DIR, exist_ok=True)

FILES = [
    (13420839, '0409-18 DPA Interview of Officer Sherry #1046 - Redacted.mp3'),
    (13420840, '0409-18 DPA Interview of Sergeant Bradford #4199 - Redacted.mp3'),
    (13420841, '0409-18 BWC of Sergeant Bradford #4199.mp4'),
    (13420842, '0409-18 BWC of Officer Sherry #1046 - Redacted.mp4'),
    (13420843, 'Production - 0409-18.pdf'),
]

total_bytes = 0
start_all = time.time()

for doc_id, filename in FILES:
    safe_name = re.sub(r'[<>:"/\\|?*#]', '_', filename)
    out_path = os.path.join(CASE_DIR, safe_name)

    if os.path.exists(out_path):
        sz = os.path.getsize(out_path)
        print(f'  [SKIP] {filename} (already exists, {sz/1024/1024:.1f} MB)')
        total_bytes += sz
        continue

    url = f'https://sfdpa.nextrequest.com/documents/{doc_id}/download'
    print(f'  [GET]  {filename}...', flush=True)
    t_start = time.time()
    try:
        with requests.get(url, stream=True, timeout=300, allow_redirects=True,
                          headers={'User-Agent': 'Mozilla/5.0'}) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            downloaded = 0
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)
        elapsed = time.time() - t_start
        mbps = (downloaded / 1024 / 1024) / elapsed if elapsed else 0
        print(f'         Done: {downloaded/1024/1024:.1f} MB in {elapsed:.0f}s ({mbps:.1f} MB/s)')
        total_bytes += downloaded
    except Exception as e:
        print(f'  [ERR]  {e}')
        if os.path.exists(out_path):
            os.remove(out_path)

total_elapsed = time.time() - start_all
print(f'\nTotal: {total_bytes/1024/1024:.1f} MB in {total_elapsed:.0f}s')
print(f'Saved to: {CASE_DIR}')
