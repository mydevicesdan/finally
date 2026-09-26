"""Ticker symbol normalization and validation."""

from __future__ import annotations

import re

# 1-6 alphanumerics starting with a letter, plus an optional share-class suffix
# ("BRK.B", "BF-B"). Deliberately permissive: the data source decides whether
# the symbol actually exists.
_TICKER_RE = re.compile(r"[A-Z][A-Z0-9]{0,5}(?:[.-][A-Z0-9]{1,2})?")


def normalize_ticker(raw: str) -> str:
    """Return the canonical (upper-case, trimmed) form of a ticker symbol.

    Raises ValueError for anything that cannot be a US equity symbol.
    """
    ticker = raw.strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


def normalize_tickers(raw: list[str]) -> list[str]:
    """Normalize a list of symbols, dropping duplicates but keeping order."""
    return list(dict.fromkeys(normalize_ticker(t) for t in raw))
