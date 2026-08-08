"""Measure per-term video/media yield to tune the harvester's search net.
Cheap: small per-term, capped file resolution. Prints a ranked table."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import muckrock_harvest as mh

PER_TERM = 6
mr = mh.MuckRock(mh.load_credentials())

TERMS = [
    "officer involved shooting", "police shooting", "critical incident",
    "critical incident video", "body camera footage", "bodycam",
    "body worn camera", "dash camera", "use of force", "use of force video",
    "in custody death", "jail death", "deadly force", "taser",
    "surveillance video", "shooting bodycam", "police video", "interrogation",
]

rows = []
for t in TERMS:
    reqs = mr.search_requests(t, "done", cap=PER_TERM)
    n_req = len(reqs)
    vid = aud = media = 0
    for r in reqs:
        fs = mr.files_for_request(r, file_cap=50)
        bk = mh.classify(fs)
        if bk["video"]:
            vid += 1
        if bk["audio"]:
            aud += 1
        if bk["video"] or bk["audio"]:
            media += 1
    rows.append((t, n_req, vid, aud, media))
    print(f"  done: {t!r}  req={n_req} vid={vid} aud={aud} media={media}", flush=True)

print("\n" + "=" * 64)
print(f"{'term':32} req  vid  aud  media")
print("=" * 64)
for t, n_req, vid, aud, media in sorted(rows, key=lambda x: (-x[4], -x[2])):
    print(f"{t:32} {n_req:3}  {vid:3}  {aud:3}  {media:4}")
