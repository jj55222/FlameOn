"""Batch-process Case 0409-18: 2 BWC videos + 2 DPA interview MP3s via Groq."""
import os
import sys
import time

# Import Pipeline 3
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pipeline3_transcribe import process_audio

CASE_DIR = 'C:/FlameON/FlameOn-main/pipeline3_audio/case_0409-18'
OUTPUT_DIR = 'C:/FlameON/FlameOn-main/pipeline3_audio/transcripts'

BATCH = [
    ('0409-18 BWC of Officer Sherry _1046 - Redacted.mp4',
     'sfdpa_0409-18_bwc_sherry', 'bodycam'),
    ('0409-18 BWC of Sergeant Bradford _4199.mp4',
     'sfdpa_0409-18_bwc_bradford', 'bodycam'),
    ('0409-18 DPA Interview of Officer Sherry _1046 - Redacted.mp3',
     'sfdpa_0409-18_interview_sherry', 'interrogation'),
    ('0409-18 DPA Interview of Sergeant Bradford _4199 - Redacted.mp3',
     'sfdpa_0409-18_interview_bradford', 'interrogation'),
]

print('=' * 80)
print(f'BATCH: Case 0409-18 — {len(BATCH)} files via Groq whisper-large-v3-turbo')
print('=' * 80)
batch_start = time.time()
results = []

for i, (filename, case_id, evidence_type) in enumerate(BATCH, 1):
    path = os.path.join(CASE_DIR, filename)
    if not os.path.exists(path):
        print(f'\n[{i}/{len(BATCH)}] SKIP — file not found: {filename}')
        continue

    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f'\n[{i}/{len(BATCH)}] {filename} ({size_mb:.1f} MB)')

    t0 = time.time()
    try:
        result = process_audio(
            source=path,
            case_id=case_id,
            output_dir=OUTPUT_DIR,
            source_evidence_type=evidence_type,
            source_url=f'https://sfdpa.nextrequest.com/requests/22-7',
            backend='groq',
            whisper_model='whisper-large-v3-turbo',
        )
        elapsed = time.time() - t0
        segs = len(result['transcript'])
        dur = result['original_duration_sec']
        results.append({
            'case_id': case_id,
            'duration_sec': dur,
            'processing_sec': elapsed,
            'segments': segs,
        })
        print(f'  Done in {elapsed:.1f}s ({dur/60:.1f} min audio, {segs} segments)')
    except Exception as e:
        print(f'  ERR: {e}')

total = time.time() - batch_start
print()
print('=' * 80)
print(f'BATCH COMPLETE — {len(results)}/{len(BATCH)} files processed in {total:.1f}s')
print('=' * 80)
total_audio = sum(r['duration_sec'] for r in results)
total_segs = sum(r['segments'] for r in results)
print(f'  Total audio processed: {total_audio/60:.1f} min')
print(f'  Total segments: {total_segs}')
print(f'  Realtime factor: {total_audio/total:.1f}x')
# Groq pricing: whisper-large-v3-turbo = $0.04/hr of audio
estimated_cost = (total_audio / 3600) * 0.04
print(f'  Estimated Groq cost: ${estimated_cost:.4f}')
print()
for r in results:
    print(f'  {r["case_id"]:40s} {r["duration_sec"]/60:>5.1f} min  {r["processing_sec"]:>5.1f}s  {r["segments"]:>4d} segs')
