"""Generate a standalone HTML audio-review page: each audio moment with its quote, an inline
player (relative to the extracted clips), and Keep/Cut toggles + an export button. Open in a
browser from .tmp/dutchess_269838/ so the relative audio_clips/ paths resolve."""
import json, os, html
from urllib.parse import quote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
G = os.path.join(ROOT, "pipeline4_scoring/golden/sac_so_20-269838_dutchess_way.golden.merged.json")
OUT = os.path.join(ROOT, ".tmp/dutchess_269838/audio_review.html")
SUGGEST_CUT = {"a02", "a06", "a09", "a11", "a18", "a19", "a08", "a10", "a12", "a29"}

d = json.load(open(G))
audio = sorted([m for m in d["moments"] if m["source_layer"] == "audio"], key=lambda x: x["moment_id"])

cards = []
for m in audio:
    mid = m["moment_id"]
    stem = m["artifact_id"][:-5] if m["artifact_id"].endswith(".json") else m["artifact_id"]
    s0 = int(m.get("start_sec") or 0)
    src = "audio_clips/" + quote(f"{mid}_{stem}_{s0}s.mp3")
    q = html.escape((m.get("evidence_quote") or "").strip())
    sal = m.get("salience")
    typ = html.escape(m.get("moment_type") or "")
    mf = "★" if m.get("must_find") else ""
    sug = mid in SUGGEST_CUT
    badge = '<span class="sugbadge">⚠ suggested cut</span>' if sug else ""
    cards.append(
        f'<div class="card" data-id="{mid}" data-keep="1" data-suggest="{1 if sug else 0}">'
        f'<div class="meta"><span class="id">{mid}</span>'
        f'<span class="sal s{sal}">s{sal}</span><span class="mf">{mf}</span>'
        f'<span class="type">{typ}</span><span class="src">{html.escape(stem)} @{s0//60}:{s0%60:02d}</span>{badge}</div>'
        f'<div class="quote">&ldquo;{q}&rdquo;</div>'
        f'<audio controls preload="none" src="{src}"></audio>'
        f'<div class="actions"><button class="b-keep" onclick="setKeep(this,1)">Keep</button>'
        f'<button class="b-cut" onclick="setKeep(this,0)">Cut</button></div></div>'
    )

TEMPLATE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dutchess audio review</title><style>
:root{--bg:#15171c;--card:#1d2027;--ink:#e6e8ee;--mut:#8a90a0;--keep:#3fb950;--cut:#f06a5a;--line:#2b2f3a;}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
header{position:sticky;top:0;background:#11131a;border-bottom:1px solid var(--line);padding:12px 18px;z-index:5;display:flex;gap:14px;align-items:center;flex-wrap:wrap}
header h1{font-size:15px;margin:0;font-weight:650}
.count{color:var(--mut)} .count b{color:var(--ink)}
button.tool{background:#262b36;color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:6px 11px;cursor:pointer;font-size:13px}
button.tool:hover{background:#2f3543}
#wrap{max-width:880px;margin:18px auto;padding:0 16px;display:flex;flex-direction:column;gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--keep);border-radius:10px;padding:12px 14px}
.card[data-keep="0"]{border-left-color:var(--cut);opacity:.5}
.meta{display:flex;gap:9px;align-items:center;font-size:12px;color:var(--mut);flex-wrap:wrap;margin-bottom:7px}
.id{font-family:ui-monospace,monospace;color:var(--ink);font-weight:700}
.sal{font-family:ui-monospace,monospace;font-weight:700}.s5{color:#f0b']}.s5{color:#ffb454}.s4{color:#7fd1ff}.s3{color:#8a90a0}
.mf{color:#ffd479}.type{background:#2b2f3a;padding:1px 7px;border-radius:5px}
.src{font-family:ui-monospace,monospace}
.sugbadge{color:var(--cut);font-weight:600}
.quote{font-size:16px;margin:4px 0 10px;color:#f4f6fb}
audio{width:100%;height:34px;filter:invert(.92) hue-rotate(180deg)}
.actions{display:flex;gap:8px;margin-top:9px}
.actions button{border:1px solid var(--line);border-radius:7px;padding:5px 16px;cursor:pointer;background:#23272f;color:var(--mut);font-weight:600}
.card[data-keep="1"] .b-keep{background:var(--keep);color:#0b1f12;border-color:var(--keep)}
.card[data-keep="0"] .b-cut{background:var(--cut);color:#2a0f0c;border-color:var(--cut)}
#exp{display:none;max-width:880px;margin:0 auto 18px;padding:0 16px}
#exp textarea{width:100%;height:120px;background:#0d0f14;color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:10px;font-family:ui-monospace,monospace;font-size:13px}
</style></head><body>
<header><h1>Dutchess audio review &mdash; %COUNT% clips</h1>
<span class="count">keep <b id="nk">0</b> &middot; cut <b id="nc">0</b></span>
<button class="tool" onclick="applySuggested()">Apply suggested cuts</button>
<button class="tool" onclick="exportDecisions()">Export decisions</button>
<button class="tool" onclick="reset()">Reset</button></header>
<div id="exp"><textarea id="exptext" readonly></textarea><div class="count">Copied to clipboard &mdash; paste back to Claude.</div></div>
<div id="wrap">%CARDS%</div>
<script>
const KEY="dutchess_audio_keep";
function load(){const s=JSON.parse(localStorage.getItem(KEY)||"{}");document.querySelectorAll('.card').forEach(c=>{const id=c.dataset.id;if(id in s)c.dataset.keep=s[id];});counts();}
function save(){const s={};document.querySelectorAll('.card').forEach(c=>s[c.dataset.id]=c.dataset.keep);localStorage.setItem(KEY,JSON.stringify(s));}
function setKeep(btn,v){btn.closest('.card').dataset.keep=v;save();counts();}
function counts(){let k=0,c=0;document.querySelectorAll('.card').forEach(x=>x.dataset.keep==="1"?k++:c++);nk.textContent=k;nc.textContent=c;}
function applySuggested(){document.querySelectorAll('.card[data-suggest="1"]').forEach(c=>c.dataset.keep="0");save();counts();}
function reset(){document.querySelectorAll('.card').forEach(c=>c.dataset.keep="1");save();counts();}
function exportDecisions(){let cut=[],keep=[];document.querySelectorAll('.card').forEach(c=>(c.dataset.keep==="0"?cut:keep).push(c.dataset.id));
const t="CUT ("+cut.length+"): "+cut.join(", ")+"\\n\\nKEEP ("+keep.length+"): "+keep.join(", ");
exptext.value=t;exp.style.display="block";navigator.clipboard&&navigator.clipboard.writeText(t).catch(()=>{});exp.scrollIntoView({behavior:"smooth"});}
load();
</script></body></html>"""

doc = TEMPLATE.replace("%CARDS%", "".join(cards)).replace("%COUNT%", str(len(audio)))
os.makedirs(os.path.dirname(OUT), exist_ok=True)
open(OUT, "w", encoding="utf-8").write(doc)
print(f"wrote {OUT} ({len(audio)} clips, {len(SUGGEST_CUT)} pre-flagged suggested cuts)")
