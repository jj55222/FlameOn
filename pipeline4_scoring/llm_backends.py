"""
llm_backends.py — LLM client abstraction for Pipeline 4.

Thin wrapper around the OpenAI SDK pointed at OpenRouter. Handles retries,
rate limits, and response cleanup (markdown fences, <think> blocks, stray prose).
Mirrors the pattern from pipeline1_winners/analyze_winner.py.
"""

import os
import re
import time
from typing import Optional


class LLMError(Exception):
    """Non-recoverable LLM error."""


class LLMBackend:
    """
    Thin facade over the OpenRouter API (OpenAI-compatible).

    Usage:
        backend = LLMBackend(model="google/gemini-3.1-flash-lite-preview")
        response_text = backend.complete(
            system="You are a...",
            user="Extract the following...",
            max_tokens=12000,
            temperature=0.1,
        )
    """

    BASE_URL = "https://openrouter.ai/api/v1"

    def __init__(
        self,
        model: str,
        api_key: Optional[str] = None,
        timeout: int = 240,
        max_retries: int = 3,
    ):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY", "")
        if not self.api_key:
            raise LLMError("OPENROUTER_API_KEY not set in environment")
        self.timeout = timeout
        self.max_retries = max_retries
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError:
                raise LLMError("openai SDK not installed. Run: pip install openai")
            self._client = OpenAI(api_key=self.api_key, base_url=self.BASE_URL)
        return self._client

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 4000,
        temperature: float = 0.2,
    ) -> str:
        """
        Send a chat completion request. Returns cleaned response text.
        Retries on parse errors, rate limits, and transient failures.
        """
        client = self._get_client()
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout=self.timeout,
                    extra_headers={
                        "HTTP-Referer": "https://github.com/jj55222/FlameOn",
                        "X-Title": "FlameOn Pipeline 4",
                    },
                )
                content = response.choices[0].message.content
                if not content:
                    raise LLMError("Empty response from LLM")
                return clean_llm_output(content)
            except Exception as e:
                last_error = e
                err_str = str(e)
                # Rate limit — longer backoff
                if "429" in err_str or "rate" in err_str.lower():
                    wait = 5 * (3 ** attempt)  # 5, 15, 45 seconds
                    print(f"  [LLM] Rate limited, waiting {wait}s before retry {attempt + 2}/{self.max_retries}...")
                    time.sleep(wait)
                else:
                    wait = 3 * (attempt + 1)
                    print(f"  [LLM] Attempt {attempt + 1}/{self.max_retries} failed: {err_str[:150]}")
                    if attempt < self.max_retries - 1:
                        time.sleep(wait)

        raise LLMError(f"All {self.max_retries} retries exhausted. Last error: {last_error}")


def _walk_open_stack(text: str) -> tuple:
    """Walk JSON text once, returning (open_stack, in_string).

    ``open_stack`` is a list of ``(close_char, open_index)`` pairs for
    every ``{`` / ``[`` that hasn't been closed by EOF. Strings and
    escape chars are tracked correctly so braces inside quoted strings
    don't perturb the stack.
    """
    open_stack: list[tuple[str, int]] = []
    in_string = False
    escape = False
    for i, ch in enumerate(text):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            open_stack.append(("}", i))
        elif ch == "[":
            open_stack.append(("]", i))
        elif ch in "}]":
            if open_stack and open_stack[-1][0] == ch:
                open_stack.pop()
    return open_stack, in_string


def repair_truncated_json(text: str) -> Optional[str]:
    """
    Best-effort repair for JSON truncated by an output-token cap.

    The common Pass 1 failure mode is the model running out of output
    tokens mid-entry of an array, producing something like::

        {"timeline": [...], "moments": [{...}, {...}, {"sou

    We walk the text tracking string state and brace positions. When
    EOF leaves any opens un-closed, we identify the start position of
    the deepest still-open structure and truncate to just before its
    preceding comma (so the partial trailing entry is dropped along
    with the dangling comma). We then append matching close tokens for
    every remaining unclosed structure.

    Returns ``None`` when the input is empty, doesn't begin with a
    JSON document start (``{`` / ``[``), or is already balanced (no
    repair needed).
    """
    if not text:
        return None
    stripped = text.strip()
    if not stripped or stripped[0] not in "{[":
        return None

    open_stack, _ = _walk_open_stack(stripped)
    if not open_stack:
        # Already balanced — the original parse must have failed for
        # some other reason (bad token, NaN, etc). Repair can't help.
        return None

    # The deepest still-open structure is the one truncation interrupted.
    # Truncate to just before its open position, drop any preceding
    # whitespace + comma + whitespace, so the partial trailing entry
    # disappears cleanly.
    partial_start = open_stack[-1][1]
    cut = partial_start
    while cut > 0 and stripped[cut - 1].isspace():
        cut -= 1
    if cut > 0 and stripped[cut - 1] == ",":
        cut -= 1
    while cut > 0 and stripped[cut - 1].isspace():
        cut -= 1

    # If the cut would land at the very start, there's nothing to keep —
    # the entire document is the partial. Can't repair.
    if cut <= 0:
        return None

    truncated = stripped[:cut]

    # Sanity: if the truncated prefix ends with ``:`` (key with no value)
    # or with ``"key"`` (no colon yet), that key is unfinished. Walk
    # back to drop it along with its preceding comma.
    truncated = _strip_dangling_key(truncated)
    if not truncated:
        return None

    # Recompute the open stack on the truncated prefix and append the
    # matching closes for whatever's still open.
    new_stack, in_str_after = _walk_open_stack(truncated)
    if in_str_after:
        # Cut landed inside a string — can't safely repair without
        # arbitrarily closing the string. Return None.
        return None
    if not new_stack:
        repaired = truncated  # already balanced after the cut
    else:
        closes = "".join(close for close, _ in reversed(new_stack))
        repaired = truncated + closes

    # Final guard: only return text that actually parses. If our
    # heuristic didn't produce valid JSON, return None rather than
    # tempt callers into a second parse failure.
    try:
        import json as _json
        _json.loads(repaired)
    except _json.JSONDecodeError:
        return None
    return repaired


def _strip_dangling_key(text: str) -> str:
    """Drop a trailing ``"key":`` or bare ``"key"`` (with no value) from
    the end of a JSON prefix, along with the preceding comma. Used by
    ``repair_truncated_json`` after the partial-entry truncation step
    to avoid generating ``{"a": }``-style invalid output.
    """
    body = text.rstrip()
    # Drop trailing colon
    if body.endswith(":"):
        body = body[:-1].rstrip()
        # Now should end with the closing `"` of the key — drop the
        # whole "key" token too.
        if body.endswith('"'):
            # Walk back to the opening `"` of this key, respecting
            # escapes.
            j = len(body) - 2
            while j >= 0:
                if body[j] == '"' and (j == 0 or body[j - 1] != "\\"):
                    body = body[:j].rstrip()
                    break
                j -= 1
            else:
                return ""
    elif body.endswith('"'):
        # Bare unfinished string at the end — drop it.
        j = len(body) - 2
        while j >= 0:
            if body[j] == '"' and (j == 0 or body[j - 1] != "\\"):
                body = body[:j].rstrip()
                break
            j -= 1
        else:
            return ""

    # Drop preceding comma if any (ahead of the dropped key).
    if body.endswith(","):
        body = body[:-1].rstrip()

    return body


def clean_llm_output(raw: str) -> str:
    """
    Strip markdown code fences, <think> blocks, and stray prose around JSON.
    Returns a string ready for json.loads(). Same pattern as analyze_winner.py.
    """
    if not raw:
        return ""

    content = raw.strip()

    # Remove <think>...</think> blocks (Qwen thinking mode)
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

    # Strip markdown code fences (```json ... ``` or ``` ... ```)
    content = re.sub(r"^```(?:json)?\s*\n?", "", content)
    content = re.sub(r"\n?```\s*$", "", content)
    content = content.strip()

    # Extract outermost JSON object/array if wrapped in prose
    # Find first { or [ and last matching } or ]
    first_brace = -1
    for i, ch in enumerate(content):
        if ch in "{[":
            first_brace = i
            break
    if first_brace > 0:
        content = content[first_brace:]

    # Trim trailing prose after JSON close
    # Walk from end to find the matching outer close
    if content and content[0] in "{[":
        open_ch = content[0]
        close_ch = "}" if open_ch == "{" else "]"
        depth = 0
        last_close = -1
        in_string = False
        escape = False
        for i, ch in enumerate(content):
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    last_close = i
                    break
        if last_close > 0:
            content = content[: last_close + 1]

    return content


def build_backend(model: str, api_key: Optional[str] = None) -> LLMBackend:
    """Factory for LLMBackend. Present for future backend swaps."""
    return LLMBackend(model=model, api_key=api_key)
