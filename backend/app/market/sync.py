"""Reconcile the data source's tracked tickers with the set the app needs."""

from __future__ import annotations

from collections.abc import Iterable

from .interface import MarketDataSource
from .tickers import normalize_ticker


async def sync_tracked_tickers(source: MarketDataSource, desired: Iterable[str]) -> None:
    """Make `source` track exactly `desired`.

    The app layer passes watchlist ∪ tickers with an open position, so a held
    ticker keeps its price after it is removed from the watchlist. Call after
    every watchlist change, trade, and LLM action batch.
    """
    wanted = {normalize_ticker(t) for t in desired}
    current = set(source.get_tickers())
    for ticker in sorted(wanted - current):
        await source.add_ticker(ticker)
    for ticker in sorted(current - wanted):
        await source.remove_ticker(ticker)
