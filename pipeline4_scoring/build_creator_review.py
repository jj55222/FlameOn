"""Extract a BWC audio clip per creator-key moment + render an HTML spot-check page
(quote + tags + player + Keep/Cut + must-find toggle). Open from .tmp/2023psb0530/."""
import json, os, subprocess, html
from urllib.parse import quote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY = os.path.join(ROOT, "pipeline4_scoring/golden/sac_so_2023psb-0530.creator_key.json")
MEDIA = os.path.expanduser("~/Downloads/2023PSB-0530")
CLIPS = os.path.join(ROOT, ".tmp/2023psb0530/clips")
OUT = os.path.join(ROOT, ".tmp/2023psb0530/creator_review.html")
os.makedirs(CLIPS, exist_ok=True)

ms = sorted(json.load(open(KEY))["moments"], key=lambda m: (-m["salience"], m["artifact_id"], m["start_sec"]))

cards = []
for m in ms:
    stem = m["artifact_id"][:-4]  # strip .mp4
    s0 = int(m["start_sec"])
    clip = f"{m['moment_id']}_{stem}_{s0}s.mp3"
    cp = os.path.join(CLIPS, clip)
    if not os.path.exists(cp):
        st = max(0, m["start_sec"] - 2.5)
        dur = (m.get("end_sec", m["start_sec"]) - m["start_sec"]) + 5
        subprocess.run(["ffmpeg", "-y", "-ss", f"{st:.2f}", "-i", os.path.join(MEDIA, m["artifact_id"]),
                        "-t", f"{dur:.2f}", "-c", "copy", cp], capture_output=True)
    q = html.escape(m["evidence_quote"][:160])
    tags = []
    if m["comment_peak"]:
        tags.append('<span class="t pk">★ comment peak</span>')
    if m["in_creators"]:
        tags.append('<span class="t">' + "+".join(("EWU" if c == "EWU" else "PT") for c in m["in_creators"]) + '</span>')
    if m.get("ewu_narration_emphasis"):
        tags.append(f'<span class="t">EWU narr×{m["ewu_narration_emphasis"]}</span>')
    cards.append(
        f'<div class="card" data-id="{m["moment_id"]}" data-keep="1" data-mf="{1 if m["must_find"] else 0}">'
        f'<div class="meta"><span class="id">{m["moment_id"]}</span><span class="sal s{m["salience"]}">s{m["salience"]}</span>'
        f'<span class="src">{html.escape(stem)} @{s0//60}:{s0%60:02d}</span>{"".join(tags)}</div>'
        f'<div class="quote">&ldquo;{q}&rdquo;</div>'
        f'<audio controls preload="none" src="clips/{quote(clip)}"></audio>'
        f'<div class="actions"><button class="b-keep" onclick="setKeep(this,1)">Keep</button>'
        f'<button class="b-cut" onclick="setKeep(this,0)">Cut</button>'
        f'<button class="b-mf" onclick="toggleMF(this)">&#9733; must-find</button></div></div>')

TEMPLATE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>2023PSB-0530 creator-key review</title><style>
:root{--bg:#15171c;--card:#1d2027;--ink:#e6e8ee;--mut:#8a90a0;--keep:#3fb950;--cut:#f06a5a;--gold:#ffd479;--line:#2b2f3a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
header{position:sticky;top:0;background:#11131a;border-bottom:1px solid var(--line);padding:11px 18px;display:flex;gap:13px;align-items:center;flex-wrap:wrap;z-index:5}
header h1{font-size:15px;margin:0}.count{color:var(--mut)}.count b{color:var(--ink)}.count .g{color:var(--gold)}
button.tool{background:#262b36;color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:6px 11px;cursor:pointer}
#wrap{max-width:900px;margin:16px auto;padding:0 16px;display:flex;flex-direction:column;gap:11px}
.card{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--keep);border-radius:10px;padding:11px 14px}
.card[data-keep="0"]{border-left-color:var(--cut);opacity:.45}
.meta{display:flex;gap:8px;align-items:center;font-size:12px;color:var(--mut);flex-wrap:wrap;margin-bottom:6px}
.id{font-family:ui-monospace,monospace;color:var(--ink);font-weight:700}.sal{font-weight:700}.s5{color:#ffb454}.s4{color:#7fd1ff}.s3{color:#8a90a0}
.src{font-family:ui-monospace,monospace}.t{background:#2b2f3a;padding:1px 7px;border-radius:5px}.t.pk{color:var(--gold)}
.card[data-mf="1"] .meta::after{content:"\\2605 must-find";color:var(--gold);font-weight:700;font-size:11px}
.quote{font-size:15.5px;margin:3px 0 8px;color:#f4f6fb}audio{width:100%;height:32px;filter:invert(.92) hue-rotate(180deg)}
.actions{display:flex;gap:8px;margin-top:8px}.actions button{border:1px solid var(--line);border-radius:7px;padding:5px 14px;cursor:pointer;background:#23272f;color:var(--mut);font-weight:600}
.card[data-keep="1"] .b-keep{background:var(--keep);color:#0b1f12}.card[data-keep="0"] .b-cut{background:var(--cut);color:#2a0f0c}
.card[data-mf="1"] .b-mf{background:var(--gold);color:#2a1e00}
#exp{display:none;max-width:900px;margin:12px auto;padding:0 16px}#exp textarea{width:100%;height:110px;background:#0d0f14;color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:10px;font-family:ui-monospace,monospace}
</style></head><body>
<header><h1>2023PSB-0530 creator-key review &mdash; %N% moments</h1>
<span class="count">keep <b id="nk">0</b> &middot; cut <b id="nc">0</b> &middot; <span class="g">&#9733; <b id="nm">0</b></span></span>
<button class="tool" onclick="clearMF()">Clear all &#9733;</button><button class="tool" onclick="exportDecisions()">Export</button><button class="tool" onclick="reset()">Reset</button></header>
<div id="exp"><textarea id="t" readonly></textarea></div><div id="wrap">%CARDS%</div>
<script>
const K="psb0530_review";
function load(){const s=JSON.parse(localStorage.getItem(K)||"{}");document.querySelectorAll('.card').forEach(c=>{const v=s[c.dataset.id];if(v){c.dataset.keep=v.k;c.dataset.mf=v.m}});cnt()}
function save(){const s={};document.querySelectorAll('.card').forEach(c=>s[c.dataset.id]={k:c.dataset.keep,m:c.dataset.mf});localStorage.setItem(K,JSON.stringify(s))}
function setKeep(b,v){b.closest('.card').dataset.keep=v;save();cnt()}
function toggleMF(b){const c=b.closest('.card');c.dataset.mf=c.dataset.mf==="1"?"0":"1";save();cnt()}
function clearMF(){document.querySelectorAll('.card').forEach(c=>c.dataset.mf="0");save();cnt()}
function reset(){document.querySelectorAll('.card').forEach(c=>c.dataset.keep="1");save();cnt()}
function cnt(){let k=0,c=0,m=0;document.querySelectorAll('.card').forEach(x=>{x.dataset.keep==="1"?k++:c++;if(x.dataset.keep==="1"&&x.dataset.mf==="1")m++});nk.textContent=k;nc.textContent=c;nm.textContent=m}
function exportDecisions(){let cut=[],mf=[];document.querySelectorAll('.card').forEach(c=>{if(c.dataset.keep==="0")cut.push(c.dataset.id);else if(c.dataset.mf==="1")mf.push(c.dataset.id)});
t.value="CUT ("+cut.length+"): "+cut.join(", ")+"\\n\\nMUST-FIND ("+mf.length+"): "+mf.join(", ");exp.style.display="block";navigator.clipboard&&navigator.clipboard.writeText(t.value).catch(()=>{});exp.scrollIntoView({behavior:"smooth"})}
load();
</script></body></html>"""

doc = TEMPLATE.replace("%CARDS%", "".join(cards)).replace("%N%", str(len(ms)))
open(OUT, "w", encoding="utf-8").write(doc)
print(f"wrote {OUT} ({len(ms)} moments, {sum(1 for m in ms if m['must_find'])} must-find, clips in {CLIPS})")
