"""Combined review page for the whole Dutchess key (doc + audio): each moment shows quote,
page-cite or audio player, Keep/Cut, and a must-find toggle — so the doc beats get pruned and
must-find gets demoted in one pass. Open from .tmp/dutchess_269838/ for relative clip paths."""
import json, os, html
from urllib.parse import quote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
G = os.path.join(ROOT, "pipeline4_scoring/golden/sac_so_20-269838_dutchess_way.golden.json")
OUT = os.path.join(ROOT, ".tmp/dutchess_269838/review.html")

d = json.load(open(G))
ms = sorted(d["moments"], key=lambda m: (0 if m["source_layer"] == "document" else 1, -(m.get("salience") or 0), m["moment_id"]))


def card(m):
    mid = m["moment_id"]; sal = m.get("salience"); typ = html.escape(m.get("moment_type") or "")
    q = html.escape((m.get("evidence_quote") or "").strip())
    mf = 1 if m.get("must_find") else 0
    if m["source_layer"] == "document":
        loc = f'p{m.get("page", "?")}'
        player = ""
        note = f'<div class="note">{html.escape(m.get("summary") or "")}</div>' if m.get("summary") else ""
    else:
        stem = m["artifact_id"][:-5] if m["artifact_id"].endswith(".json") else m["artifact_id"]
        s0 = int(m.get("start_sec") or 0)
        loc = f'{html.escape(stem)} @{s0//60}:{s0%60:02d}'
        player = f'<audio controls preload="none" src="audio_clips/{quote(f"{mid}_{stem}_{s0}s.mp3")}"></audio>'
        note = ""
    return (
        f'<div class="card" data-id="{mid}" data-keep="1" data-mf="{mf}">'
        f'<div class="meta"><span class="id">{mid}</span><span class="sal s{sal}">s{sal}</span>'
        f'<span class="type">{typ}</span><span class="src">{loc}</span></div>'
        f'<div class="quote">&ldquo;{q}&rdquo;</div>{note}{player}'
        f'<div class="actions"><button class="b-keep" onclick="setKeep(this,1)">Keep</button>'
        f'<button class="b-cut" onclick="setKeep(this,0)">Cut</button>'
        f'<button class="b-mf" onclick="toggleMF(this)">&#9733; must-find</button></div></div>'
    )


docs = [card(m) for m in ms if m["source_layer"] == "document"]
auds = [card(m) for m in ms if m["source_layer"] == "audio"]

TEMPLATE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Dutchess key review</title><style>
:root{--bg:#15171c;--card:#1d2027;--ink:#e6e8ee;--mut:#8a90a0;--keep:#3fb950;--cut:#f06a5a;--gold:#ffd479;--line:#2b2f3a;}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
header{position:sticky;top:0;background:#11131a;border-bottom:1px solid var(--line);padding:11px 18px;z-index:5;display:flex;gap:13px;align-items:center;flex-wrap:wrap}
header h1{font-size:15px;margin:0;font-weight:650}
.count{color:var(--mut)}.count b{color:var(--ink)} .count .g{color:var(--gold)}
button.tool{background:#262b36;color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:6px 11px;cursor:pointer;font-size:13px}
button.tool:hover{background:#2f3543}
h2{max-width:900px;margin:22px auto 2px;padding:0 16px;font-size:13px;color:var(--mut);text-transform:uppercase;letter-spacing:.5px}
#wrap,.sec{max-width:900px;margin:0 auto;padding:8px 16px;display:flex;flex-direction:column;gap:11px}
.card{background:var(--card);border:1px solid var(--line);border-left:4px solid var(--keep);border-radius:10px;padding:11px 14px}
.card[data-keep="0"]{border-left-color:var(--cut);opacity:.45}
.meta{display:flex;gap:9px;align-items:center;font-size:12px;color:var(--mut);flex-wrap:wrap;margin-bottom:6px}
.id{font-family:ui-monospace,monospace;color:var(--ink);font-weight:700}
.sal{font-family:ui-monospace,monospace;font-weight:700}.s5{color:#ffb454}.s4{color:#7fd1ff}.s3{color:#8a90a0}
.type{background:#2b2f3a;padding:1px 7px;border-radius:5px}.src{font-family:ui-monospace,monospace}
.card[data-mf="1"] .meta::after{content:"\\2605 must-find";color:var(--gold);font-weight:700;font-size:11px}
.quote{font-size:15.5px;margin:3px 0 7px;color:#f4f6fb}
.note{font-size:12.5px;color:var(--mut);margin:0 0 8px;border-left:2px solid var(--line);padding-left:9px}
audio{width:100%;height:32px;filter:invert(.92) hue-rotate(180deg)}
.actions{display:flex;gap:8px;margin-top:9px;flex-wrap:wrap}
.actions button{border:1px solid var(--line);border-radius:7px;padding:5px 14px;cursor:pointer;background:#23272f;color:var(--mut);font-weight:600}
.card[data-keep="1"] .b-keep{background:var(--keep);color:#0b1f12;border-color:var(--keep)}
.card[data-keep="0"] .b-cut{background:var(--cut);color:#2a0f0c;border-color:var(--cut)}
.card[data-mf="1"] .b-mf{background:var(--gold);color:#2a1e00;border-color:var(--gold)}
#exp{display:none;max-width:900px;margin:14px auto;padding:0 16px}
#exp textarea{width:100%;height:120px;background:#0d0f14;color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:10px;font-family:ui-monospace,monospace;font-size:13px}
</style></head><body>
<header><h1>Dutchess key review &mdash; %N% moments</h1>
<span class="count">keep <b id="nk">0</b> &middot; cut <b id="nc">0</b> &middot; <span class="g">&#9733; <b id="nm">0</b> must-find</span></span>
<button class="tool" onclick="clearMF()">Clear all &#9733;</button>
<button class="tool" onclick="exportDecisions()">Export decisions</button>
<button class="tool" onclick="reset()">Reset</button></header>
<div id="exp"><textarea id="exptext" readonly></textarea><div class="count">Copied &mdash; paste back to Claude.</div></div>
<h2>Document beats (%ND%) &mdash; LLM-judged, not yet human-reviewed</h2><div class="sec">%DOCS%</div>
<h2>Audio beats (%NA%) &mdash; already keep/cut-reviewed; set must-find here</h2><div class="sec">%AUDS%</div>
<script>
const KEY="dutchess_review_v2";
function load(){const s=JSON.parse(localStorage.getItem(KEY)||"{}");document.querySelectorAll('.card').forEach(c=>{const v=s[c.dataset.id];if(v){c.dataset.keep=v.k;c.dataset.mf=v.m;}});counts();}
function save(){const s={};document.querySelectorAll('.card').forEach(c=>s[c.dataset.id]={k:c.dataset.keep,m:c.dataset.mf});localStorage.setItem(KEY,JSON.stringify(s));}
function setKeep(b,v){b.closest('.card').dataset.keep=v;save();counts();}
function toggleMF(b){const c=b.closest('.card');c.dataset.mf=c.dataset.mf==="1"?"0":"1";save();counts();}
function clearMF(){document.querySelectorAll('.card').forEach(c=>c.dataset.mf="0");save();counts();}
function reset(){document.querySelectorAll('.card').forEach(c=>{c.dataset.keep="1";});save();counts();}
function counts(){let k=0,c=0,m=0;document.querySelectorAll('.card').forEach(x=>{x.dataset.keep==="1"?k++:c++;if(x.dataset.keep==="1"&&x.dataset.mf==="1")m++;});nk.textContent=k;nc.textContent=c;nm.textContent=m;}
function exportDecisions(){let cut=[],mf=[];document.querySelectorAll('.card').forEach(c=>{if(c.dataset.keep==="0")cut.push(c.dataset.id);else if(c.dataset.mf==="1")mf.push(c.dataset.id);});
const t="CUT ("+cut.length+"): "+cut.join(", ")+"\\n\\nMUST-FIND ("+mf.length+"): "+mf.join(", ");
exptext.value=t;exp.style.display="block";navigator.clipboard&&navigator.clipboard.writeText(t).catch(()=>{});exp.scrollIntoView({behavior:"smooth"});}
load();
</script></body></html>"""

doc_html = (TEMPLATE.replace("%DOCS%", "".join(docs)).replace("%AUDS%", "".join(auds))
            .replace("%N%", str(len(ms))).replace("%ND%", str(len(docs))).replace("%NA%", str(len(auds))))
os.makedirs(os.path.dirname(OUT), exist_ok=True)
open(OUT, "w", encoding="utf-8").write(doc_html)
print(f"wrote {OUT} ({len(ms)} moments: {len(docs)} doc + {len(auds)} audio)")
