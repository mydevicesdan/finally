"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule; nobody asks a source for a price. Downstream code reads the cache.

    Contract every implementation honours:
      - Ticker arguments are normalized with normalize_ticker() (upper-case,
        trimmed, validated); invalid symbols raise ValueError.
      - start() is called once; a second call raises RuntimeError.
      - add_ticker()/remove_ticker() are idempotent and may be called before
        start() — the ticker is then included when the source starts.
      - remove_ticker() also removes the ticker from the PriceCache, and no
        in-flight update may re-insert it afterwards.
      - stop() is idempotent; after it returns the source never writes again.
      - Background failures are logged and retried, never raised.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices for `tickers` (plus any added before start)."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and release resources."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and drop it from the cache. No-op if unknown."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers, in insertion order."""
