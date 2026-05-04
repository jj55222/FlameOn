"""Per-template HTML extractors for portal-live raw payloads.

Each module here knows how to convert one specific agency's HTML
template into the canonical ``agency_ois`` fixture shape that the
existing ``--portal-replay`` path consumes. Extractors are pure: they
take an HTML string + the source URL and return a plain dict, never
touching the network or disk.

The dispatch layer in ``portal_live_fetch.extract_to_agency_ois``
picks an extractor by URL host + body-class fingerprint. New
extractors are added by:

  1. Writing a ``<agency>_<template>_html.py`` module here that
     exposes a ``is_<template>`` predicate and an
     ``extract_<template>_to_agency_ois`` function.
  2. Registering it in ``portal_live_fetch.extract_to_agency_ois``'s
     dispatch table.
  3. Adding a saved real-page HTML fixture under
     ``tests/fixtures/portal_live_html/`` and the corresponding tests.
"""
from __future__ import annotations

from .phoenix_newsroom_html import (
    extract_phoenix_newsroom_to_agency_ois,
    is_phoenix_newsroom_article_detail,
)


__all__ = [
    "extract_phoenix_newsroom_to_agency_ois",
    "is_phoenix_newsroom_article_detail",
]
