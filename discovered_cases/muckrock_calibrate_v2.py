"""Calibration v2 — broaden beyond OIS: BWC, interrogation, and SB16/1421-style
accountability artifacts. Per term: video/audio/doc yield + artifact signals
sniffed from file titles. Goal: see what MuckRock actually holds."""
import re, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import muckrock_harvest as mh

PER_TERM = 6
mr = mh.MuckRock(mh.load_credentials())

SB16 = re.compile(r"\b(sb[\s-]?(1421|16)|senate bill (1421|16)|832\.7|peace officer.*record|personnel record)", re.I)
INTERR = re.compile(r"\b(interrogat|interview|custodial)", re.I)
BWC = re.compile(r"\b(body[\s-]?worn|bwc|body[\s-]?cam|axon|in[\s-]?car|dash[\s-]?cam)", re.I)

TERMS = [
    # baselines (known media-yielders)
    "dash camera", "body camera footage",
    # general BWC (not OIS-specific)
    "body worn camera", "body camera", "police body camera",
    # interrogation / interview
    "interrogation", "interrogation video", "interview", "custodial interview",
    # SB16 / SB1421 / accountability-record artifacts
    "SB 1421", "SB1421", "Senate Bill 1421", "SB 16",
    "police misconduct records", "use of force records",
    "peace officer personnel records", "RIPA", "Brady list",
]

rows = []
for t in TERMS:
    reqs = mr.search_requests(t, "done", cap=PER_TERM)
    vid = aud = doc = sb = interr = bwc = 0
    for r in reqs:
        fs = mr.files_for_request(r, file_cap=60)
        bk = mh.classify(fs)
        titles = " ".join((f.get("title") or "") + " " + (f.get("ffile") or "") for f in fs)
        title_all = (r.get("title") or "") + " " + titles
        if bk["video"]: vid += 1
        if bk["audio"]: aud += 1
        if bk["doc"]:   doc += 1
        if SB16.search(title_all):   sb += 1
        if INTERR.search(title_all): interr += 1
        if BWC.search(title_all):    bwc += 1
    rows.append((t, len(reqs), vid, aud, doc, sb, interr, bwc))
    print(f"  {t!r}: req={len(reqs)} vid={vid} aud={aud} doc={doc} | sb16={sb} interr={interr} bwc={bwc}", flush=True)

print("\n" + "=" * 86)
print(f"{'term':30} req vid aud doc | sb16 intr bwc")
print("=" * 86)
for t, n, vid, aud, doc, sb, interr, bwc in sorted(rows, key=lambda x: (-(x[2]+x[3]), -x[4])):
    print(f"{t:30} {n:3} {vid:3} {aud:3} {doc:3} | {sb:4} {interr:4} {bwc:3}")
