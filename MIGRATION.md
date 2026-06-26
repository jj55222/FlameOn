# FlameOn — Migration to a New Machine

Everything needed to continue the **entire** project (all pipelines + the OIS
documentary work) on a new computer. Work through the steps in order.

> Windows paths below (`C:\FlameON\FlameOn-main`, `C:\Users\Diner\...`) are the OLD
> machine. The new machine is a **Mac** — use the macOS quickstart immediately below;
> the detailed steps afterward are the same, just translate the paths.

---

## ⚡ macOS quickstart (the new machine)

The 40 GB evidence zip is already on the Mac (e.g. `~/Downloads/2017-289964.zip`) — so
the one heavy copy is done. Everything else:

```bash
# 1. Code (private repo — you'll be prompted to auth GitHub)
git clone https://github.com/jj55222/FlameOn.git ~/FlameOn-main
cd ~/FlameOn-main && git checkout p6-documentary-assembly

# 2. Gitignored essentials: copy flameon_migration_bundle_2026-06-26.zip to the Mac, then
unzip -o ~/Downloads/flameon_migration_bundle_2026-06-26.zip -d ~/FlameOn-main
#    → restores .tmp/ (handoffs, goal docs, analysis JSONs), .env, and claude_memory/

# 3. Python env (venv recommended on macOS)
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install torch torchvision torchaudio      # Mac: default wheels (Apple-Silicon MPS / CPU) — NO cuda index
pip install -r requirements.txt

# 4. ffmpeg (imageio-ffmpeg from requirements usually suffices; brew is the fallback)
brew install ffmpeg

# 5. Verify
cd pipeline3_audio && python -m pytest -q     # expect ~76 passing
```

**Point tools at the Mac zip path**, e.g.:
```bash
python pipeline3_audio/find_shooting_pov.py --zip ~/Downloads/2017-289964.zip \
  --triage .tmp/ois_289964/zip_triage_full.json --top-n 136 --budget-usd 0.50
```

**Claude auto-memory on macOS** (the path-mangling differs from Windows, so don't guess):
1. Launch `claude` once inside `~/FlameOn-main` — it creates `~/.claude/projects/<mangled>/`.
2. `ls ~/.claude/projects/` to find that folder, then:
   `cp ~/FlameOn-main/claude_memory/* ~/.claude/projects/<mangled>/memory/`
   (the bundle unpacks the 11 memory files to `~/FlameOn-main/claude_memory/`).

**Rotate the `OPENROUTER_API_KEY`** in `.env` (Step 4 below). Done — skip to "Read-first".

---

## What you're moving — three buckets

| Bucket | How it travels | Size |
|---|---|---|
| **1. Code + full git history** (all pipelines, autoresearch, the 3 new finders) | **git** (push → clone) | ~465 MB `.git` |
| **2. Gitignored essentials** (handoffs, goal docs, analysis JSONs, `.env`, auto-memory) | **transfer bundle zip** | a few MB |
| **3. Large external data** (the 40 GB OIS evidence zip, model caches) | **manual copy / re-download** | ~40 GB |

Git carries the code; the bundle carries the knowledge + secrets git deliberately
excludes; the big evidence zip is copied by hand (too large for git).

---

## Step 1 — Code (do this FIRST; it's also your only backup)

⚠️ **The working branch `p6-documentary-assembly` has 273 commits that were NEVER
pushed.** Right now this machine is the only copy. Before anything else:

```bash
# on the OLD machine
cd C:\FlameON\FlameOn-main
git push -u origin p6-documentary-assembly
```

Then on the **new** machine:

```bash
git clone https://github.com/jj55222/FlameOn.git C:\FlameON\FlameOn-main
cd C:\FlameON\FlameOn-main
git checkout p6-documentary-assembly
```

(If you'd rather not push case material to GitHub, instead copy the whole
`C:\FlameON\FlameOn-main` folder — including `.git` — to the new machine.)

---

## Step 2 — Unpack the transfer bundle

Copy `flameon_migration_bundle_<date>.zip` (created next to the repo) to the new
machine and unzip it **over the repo root**. It restores:

- `.tmp/` — all handoffs, the `GOAL_A/B/C/D` docs, phase plans, and the OIS
  analysis outputs (`zip_triage_full.json`, `pov_vision*.json`, `motion_full.json`,
  `aerial_*.json`, logs). **This is the project's working memory.**
- `.env` — your API keys (see Step 4 — **rotate the OpenRouter key**).
- `brave_quota.json` — Brave billing state (don't reset mid-month).
- `claude_memory/` — the 11 Claude auto-memory files (place per Step 5).

The bundle deliberately **excludes** media/frames (`.mp4/.png/.wav`) — those are
regenerable scratch.

---

## Step 3 — Large external data (manual copy)

| File | Path | Size | Note |
|---|---|---|---|
| OIS evidence bundle | `C:\Users\Diner\Downloads\2017-289964.zip` | **40 GB** | the 266-video FOIA zip; copy to the same path or update paths in commands |
| HF / easyocr / whisper model caches | `~/.cache/huggingface`, `~/.EasyOCR` | varies | optional — they auto-redownload on first run |

Copy the 40 GB zip via USB/external drive or cloud. Nothing in the code unzips it;
all tools stream selectively from it, so it can live anywhere — just point
`--zip` at its new location.

---

## Step 4 — Environment

```bash
# 1. PyTorch first (pick CPU or CUDA for the new machine's hardware)
python -m pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
# 2. everything else
python -m pip install -r requirements.txt
```

**`.env` keys** (restored by the bundle; recreate if needed):
`OPENROUTER_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `BRAVE_API_KEY`,
`COURTLISTENER_API_KEY`, `MUCKROCK_API_TOKEN`, `HF_TOKEN`, and optional
`BRAVE_SPEND_LIMIT_USD` (default 4.00).

> 🔐 **Rotate the `OPENROUTER_API_KEY`** — it's operator-provided and has been used
> across sessions. Generate a fresh one at openrouter.ai and update `.env`.

ffmpeg/ffprobe: provided by `imageio-ffmpeg` (in requirements) or a system ffmpeg
on PATH; `pov_triage._ff()` resolves either.

---

## Step 5 — Claude Code auto-memory

The 11 memory files live OUTSIDE the repo. Place the bundle's `claude_memory/`
contents at:

```
C:\Users\<you>\.claude\projects\C--FlameON-FlameOn-main\memory\
```

That folder name is derived from the repo path, so keeping the repo at
`C:\FlameON\FlameOn-main` makes it line up. `MEMORY.md` is the index Claude loads
each session.

---

## Step 6 — Verify the new machine

```bash
cd C:\FlameON\FlameOn-main\pipeline3_audio && python -m pytest -q      # expect ~76 passing
cd C:\FlameON\FlameOn-main\autoresearch   && python evaluate.py --case 4 --verbose
```

If both run, the environment is good.

---

## Read-first, in this order

1. `.tmp/HANDOFF_OIS_2026-06-25.md` — current state of the OIS documentary engine + the gunshot-finding work.
2. `autoresearch/CLAUDE.md` — the autoresearch loop rules.
3. `.tmp/GOAL_D_*.md`, `.tmp/GOAL_A/B/C_*.md` — the /loop-able goal docs.
4. The auto-memory `MEMORY.md` index.

## Where the OIS case stands (so you don't repeat it)

- **266 videos fully triaged**: audio (100%, no gunshot volley anywhere), motion-salience
  (136/136 full timeline), vision (all small clips + loud-window on the 136). Conclusion:
  **no camera captured the live gunfire** — only response/pursuit/aftermath/evidence.
- **Three sound-free finders shipped** (`pipeline3_audio/`): `motion_triage.py`,
  `motion_sync.py`, `aerial_telemetry.py` (commit `496d7b1`).
- **Open thread:** the footage shows "El Camino Ave" while docs say "Auburn Blvd/Ramada" —
  the bundle may be a different/mixed incident. Resolve via the 1793-page packet (OCR pending).
