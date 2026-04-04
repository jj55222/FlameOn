"""
brave_guard.py — IMMUTABLE Brave API billing guard

DO NOT MODIFY THIS FILE. It exists to prevent runaway Brave API spend.
If you need to change billing limits, set the BRAVE_SPEND_LIMIT_USD env var.

Usage:
    from brave_guard import brave_request

    # Instead of calling requests.get() directly:
    response = brave_request(url, params, headers)
    # Returns None if blocked by guard. Always check for None.
"""

import json
import os
import time
import requests
from pathlib import Path
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────
QUOTA_FILE = Path(__file__).parent / "brave_quota.json"
DEFAULT_SPEND_LIMIT = 4.00        # USD — override with BRAVE_SPEND_LIMIT_USD env var
COST_PER_REQUEST = 0.005          # USD per Brave API call
RATE_LIMIT_DELAY = 1.1            # seconds between calls (Brave limit: 1 req/sec)
ABSOLUTE_HARD_CEILING = 10.00     # USD — NEVER spend more than this in a month, period.
                                  # This is the last line of defense. Even if someone
                                  # sets BRAVE_SPEND_LIMIT_USD to $100, this cap holds.

# ─── Quota state ─────────────────────────────────────────────────────────────

def _current_month_key():
    return datetime.utcnow().strftime("%Y-%m")


def _load_quota():
    """Load quota state from disk. Create fresh if missing or month changed."""
    default = {
        "month_key": _current_month_key(),
        "monthly_remaining": None,  # Unknown until first response header
        "estimated_spend": 0.0,
        "calls_this_month": 0,
        "blocked_calls": 0,        # How many calls were blocked by this guard
        "last_call_at": None,
    }

    if not QUOTA_FILE.exists():
        _save_quota(default)
        return default

    try:
        with open(QUOTA_FILE, "r") as f:
            quota = json.load(f)
    except (json.JSONDecodeError, IOError):
        # Corrupted file — start fresh but log it
        print("[BRAVE GUARD] WARNING: brave_quota.json corrupted, resetting")
        _save_quota(default)
        return default

    # Month rollover — reset counters
    if quota.get("month_key") != _current_month_key():
        print(f"[BRAVE GUARD] New month ({_current_month_key()}), resetting counters")
        default["month_key"] = _current_month_key()
        _save_quota(default)
        return default

    return quota


def _save_quota(quota):
    """Persist quota state to disk. Atomic write to prevent corruption."""
    tmp_path = QUOTA_FILE.with_suffix(".tmp")
    try:
        with open(tmp_path, "w") as f:
            json.dump(quota, f, indent=2)
        tmp_path.replace(QUOTA_FILE)
    except IOError as e:
        print(f"[BRAVE GUARD] ERROR: Could not save quota file: {e}")


def _get_spend_limit():
    """Get the effective spend limit. Capped by ABSOLUTE_HARD_CEILING."""
    env_limit = os.environ.get("BRAVE_SPEND_LIMIT_USD")
    if env_limit:
        try:
            limit = float(env_limit)
        except ValueError:
            print(f"[BRAVE GUARD] WARNING: Invalid BRAVE_SPEND_LIMIT_USD '{env_limit}', using default ${DEFAULT_SPEND_LIMIT}")
            limit = DEFAULT_SPEND_LIMIT
    else:
        limit = DEFAULT_SPEND_LIMIT

    # Hard ceiling — even if env var says $100, we cap at ABSOLUTE_HARD_CEILING
    if limit > ABSOLUTE_HARD_CEILING:
        print(f"[BRAVE GUARD] WARNING: BRAVE_SPEND_LIMIT_USD (${limit}) exceeds hard ceiling (${ABSOLUTE_HARD_CEILING}). Capping.")
        limit = ABSOLUTE_HARD_CEILING

    return limit


# ─── Pre-flight checks ──────────────────────────────────────────────────────

def _check_allowed(quota):
    """
    Returns (allowed: bool, reason: str).
    ALL checks must pass for a call to proceed.
    """
    spend_limit = _get_spend_limit()

    # Check 1: Spend cap
    projected = quota["estimated_spend"] + COST_PER_REQUEST
    if projected > spend_limit:
        return False, f"Spend cap reached: ${quota['estimated_spend']:.3f} + ${COST_PER_REQUEST} > ${spend_limit:.2f} limit"

    # Check 2: Hard ceiling (defense in depth)
    if projected > ABSOLUTE_HARD_CEILING:
        return False, f"ABSOLUTE HARD CEILING: ${projected:.3f} would exceed ${ABSOLUTE_HARD_CEILING:.2f}"

    # Check 3: Brave's own monthly remaining (from response headers)
    if quota.get("monthly_remaining") is not None and quota["monthly_remaining"] <= 0:
        return False, f"Brave monthly quota exhausted (x-ratelimit-remaining = 0)"

    return True, "OK"


# ─── Public API ──────────────────────────────────────────────────────────────

def brave_request(url, params=None, headers=None, timeout=10):
    """
    Make a guarded Brave API request.

    Returns:
        requests.Response on success
        None if blocked by guard (call was NOT made)

    Raises:
        Nothing — all errors are caught and logged. Returns None on failure.
    """
    quota = _load_quota()

    # Pre-flight check
    allowed, reason = _check_allowed(quota)
    if not allowed:
        quota["blocked_calls"] = quota.get("blocked_calls", 0) + 1
        _save_quota(quota)
        print(f"[BRAVE GUARD] BLOCKED: {reason}")
        print(f"[BRAVE GUARD] Month stats: {quota['calls_this_month']} calls, ${quota['estimated_spend']:.3f} spent, {quota['blocked_calls']} blocked")
        return None

    # Rate limiting
    last_call = quota.get("last_call_at")
    if last_call:
        elapsed = time.time() - last_call
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

    # Make the actual request
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        print(f"[BRAVE GUARD] Request error: {e}")
        return None

    # Update quota AFTER successful request
    quota["calls_this_month"] += 1
    quota["estimated_spend"] += COST_PER_REQUEST
    quota["last_call_at"] = time.time()

    # Read Brave's rate limit headers
    ratelimit_remaining = response.headers.get("x-ratelimit-remaining")
    if ratelimit_remaining:
        try:
            # Brave returns comma-separated values; second value is monthly remaining
            parts = ratelimit_remaining.split(",")
            if len(parts) >= 2:
                quota["monthly_remaining"] = int(parts[1].strip())
            else:
                quota["monthly_remaining"] = int(parts[0].strip())
        except (ValueError, IndexError):
            pass  # Don't crash on unparseable headers

    # Handle 402 (quota exceeded on Brave's side)
    if response.status_code == 402:
        print("[BRAVE GUARD] HTTP 402 — Brave quota exceeded. Blocking all future calls this month.")
        quota["monthly_remaining"] = 0

    _save_quota(quota)

    # Log every 10 calls
    if quota["calls_this_month"] % 10 == 0:
        print(f"[BRAVE GUARD] Progress: {quota['calls_this_month']} calls, ${quota['estimated_spend']:.3f} spent, remaining: {quota.get('monthly_remaining', '?')}")

    return response


def get_quota_status():
    """Return current quota state for inspection."""
    quota = _load_quota()
    spend_limit = _get_spend_limit()
    allowed, reason = _check_allowed(quota)
    return {
        **quota,
        "spend_limit": spend_limit,
        "hard_ceiling": ABSOLUTE_HARD_CEILING,
        "calls_remaining_by_budget": max(0, int((spend_limit - quota["estimated_spend"]) / COST_PER_REQUEST)),
        "currently_allowed": allowed,
        "block_reason": reason if not allowed else None,
    }


def print_quota_status():
    """Pretty-print current quota status."""
    s = get_quota_status()
    print(f"\n{'='*50}")
    print(f"  BRAVE API QUOTA STATUS — {s['month_key']}")
    print(f"{'='*50}")
    print(f"  Calls made:      {s['calls_this_month']}")
    print(f"  Estimated spend: ${s['estimated_spend']:.3f}")
    print(f"  Spend limit:     ${s['spend_limit']:.2f}")
    print(f"  Hard ceiling:    ${s['hard_ceiling']:.2f}")
    print(f"  Budget remaining: ~{s['calls_remaining_by_budget']} calls")
    print(f"  Brave remaining: {s.get('monthly_remaining', 'unknown')}")
    print(f"  Blocked calls:   {s.get('blocked_calls', 0)}")
    print(f"  Status:          {'✓ ALLOWED' if s['currently_allowed'] else '✗ BLOCKED — ' + s['block_reason']}")
    print(f"{'='*50}\n")


# ─── Standalone check ────────────────────────────────────────────────────────
if __name__ == "__main__":
    print_quota_status()
