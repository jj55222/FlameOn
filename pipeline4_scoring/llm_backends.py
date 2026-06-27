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
        self.last_reasoning = None   # reasoning trace from the most recent complete() call
        self.last_raw = None         # raw (pre-clean) content from the most recent call

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
                if not getattr(response, "choices", None):
                    raise LLMError("No choices in response (provider returned empty/error)")
                msg = response.choices[0].message
                content = msg.content
                # Capture the reasoning trace instead of discarding it. OpenRouter exposes it on
                # msg.reasoning for many reasoning models; some inline it as <think>...</think>.
                self.last_reasoning = getattr(msg, "reasoning", None) or extract_think(content)
                self.last_raw = content
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
