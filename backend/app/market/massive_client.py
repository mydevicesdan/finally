"""Massive (formerly Polygon.io) REST poller for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import SnapshotMarketType, TickerSnapshot

from .cache import PriceCache
from .interface import MarketDataSource
from .tickers import normalize_ticker, normalize_tickers

logger = logging.getLogger(__name__)

FREE_TIER_MIN_SPACING = 12.0  # 5 requests/minute
MAX_BACKOFF = 120.0  # seconds added to the poll interval after repeated failures


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float  # Unix seconds
    session_open: float | None  # previous close, if the snapshot has one


def parse_snapshot(snap: TickerSnapshot) -> ParsedQuote | None:
    """Turn one massive TickerSnapshot into a ParsedQuote, or None if unusable.

    Price preference: last trade -> current minute bar close -> today's close
    -> previous day's close (weekends / before first trade of the day).
    Massive timestamps: last_trade.sip_timestamp and snapshot.updated are Unix
    NANOseconds; min.timestamp is Unix MILLIseconds.
    """
    if not snap.ticker:
        return None

    price: float | None = None
    timestamp: float | None = None

    trade = snap.last_trade
    if trade is not None and trade.price:
        price = float(trade.price)
        ns = trade.sip_timestamp or trade.participant_timestamp
        if ns:
            timestamp = ns / 1e9
    if price is None and snap.min is not None and snap.min.close:
        price = float(snap.min.close)
        if snap.min.timestamp:
            timestamp = snap.min.timestamp / 1e3
    if price is None and snap.day is not None and snap.day.close:
        price = float(snap.day.close)
    if price is None and snap.prev_day is not None and snap.prev_day.close:
        price = float(snap.prev_day.close)
    if price is None:
        return None

    if timestamp is None:
        timestamp = snap.updated / 1e9 if snap.updated else time.time()

    session_open = None
    if snap.prev_day is not None and snap.prev_day.close:
        session_open = float(snap.prev_day.close)

    return ParsedQuote(snap.ticker, price, timestamp, session_open)


def classify_error(exc: BaseException) -> str:
    """Bucket a poll failure: 'auth', 'forbidden', 'rate_limit' or 'transient'.

    BadResponse carries only the response body (no status code), so we match
    on the body text Massive returns.
    """
    if isinstance(exc, AuthError):
        return "auth"
    if isinstance(exc, BadResponse):
        body = str(exc).lower()
        if "not_authorized" in body or "not entitled" in body:
            return "forbidden"
        if "exceeded the maximum requests" in body or "429" in body:
            return "rate_limit"
        if "unknown api key" in body or "invalid api key" in body:
            return "auth"
    return "transient"


class MassiveDataSource(MarketDataSource):
    """Polls the Massive full-market snapshot endpoint for all tracked tickers.

    One request per cycle regardless of ticker count:
        GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
    """

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_poll_spacing: float | None = None,
        client: RESTClient | None = None,
    ) -> None:
        # retries=0: urllib3 would otherwise retry 429s three times, burning the
        # free tier's 5 req/min budget. Our loop is the retry mechanism.
        self._client = client or RESTClient(
            api_key=api_key, retries=0, connect_timeout=5.0, read_timeout=10.0
        )
        self._cache = price_cache
        self._interval = poll_interval
        self._min_spacing = (
            min(poll_interval, FREE_TIER_MIN_SPACING)
            if min_poll_spacing is None
            else min_poll_spacing
        )
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()  # set by add_ticker() to poll early
        self._last_poll = float("-inf")  # time.monotonic() of the last request
        self._backoff = 0.0
        self._failures = 0
        self._missing_warned: set[str] = set()

    # --- MarketDataSource ---

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource already started")
        for ticker in normalize_tickers(tickers):
            if ticker not in self._tickers:
                self._tickers.append(ticker)
        await self._poll_once()  # prime the cache before the first SSE push
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval", len(self._tickers), self._interval
        )

    async def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            return
        self._tickers.append(ticker)
        self._missing_warned.discard(ticker)
        self._wake.set()  # fetch it on the next allowed slot, not in up to 15s

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker in self._tickers:
            self._tickers.remove(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval + self._backoff)
            except TimeoutError:
                pass
            self._wake.clear()
            # Never exceed the rate limit, even when woken early by add_ticker().
            spacing = max(self._min_spacing, self._backoff)
            wait = spacing - (time.monotonic() - self._last_poll)
            if wait > 0:
                await asyncio.sleep(wait)
            await self._poll_once()

    async def _poll_once(self) -> None:
        tickers = list(self._tickers)
        if not tickers:
            return
        self._last_poll = time.monotonic()
        try:
            # RESTClient is synchronous (urllib3) - keep it off the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except Exception as exc:
            self._record_failure(exc)
            return
        self._record_success()

        # Re-read the tracked set: a ticker removed while the request was in
        # flight must not be written back into the cache.
        tracked = set(self._tickers)
        seen: set[str] = set()
        for snap in snapshots:
            quote = parse_snapshot(snap)
            if quote is None:
                logger.warning("Unusable Massive snapshot for %s", getattr(snap, "ticker", "?"))
                continue
            seen.add(quote.ticker)
            if quote.ticker not in tracked:
                continue
            self._cache.update(
                quote.ticker,
                quote.price,
                timestamp=quote.timestamp,
                session_open=quote.session_open,
            )

        for ticker in (set(tickers) & tracked) - seen - self._missing_warned:
            logger.warning("Massive returned no data for %s (unknown symbol?)", ticker)
            self._missing_warned.add(ticker)

    def _fetch_snapshots(self, tickers: list[str]) -> list[TickerSnapshot]:
        """Blocking HTTP call; runs in a worker thread."""
        return self._client.get_snapshot_all(
            # Pass the string value: SnapshotMarketType is a plain Enum in massive
            # 2.x, and the SDK formats the enum member itself into the URL
            # ("/locale/global/markets/SnapshotMarketType.STOCKS/...").
            market_type=SnapshotMarketType.STOCKS.value,
            tickers=tickers,
        )

    def _record_success(self) -> None:
        if self._failures:
            logger.info("Massive poll recovered after %d failure(s)", self._failures)
        self._failures = 0
        self._backoff = 0.0

    def _record_failure(self, exc: Exception) -> None:
        self._failures += 1
        kind = classify_error(exc)
        if kind in ("auth", "forbidden"):
            # Retrying fast will not fix a bad key or a plan without snapshot
            # access. Keep trying slowly so a fixed key/plan recovers by itself.
            self._backoff = MAX_BACKOFF
            if self._failures == 1:
                logger.error(
                    "Massive %s error - check MASSIVE_API_KEY and that your plan includes "
                    "stock snapshots, or unset MASSIVE_API_KEY to use the simulator: %s",
                    kind,
                    exc,
                )
            return
        # rate_limit / transient: exponential backoff 15s, 30s, 60s, 120s ...
        self._backoff = min(MAX_BACKOFF, self._interval * 2 ** min(self._failures - 1, 3))
        logger.warning(
            "Massive poll failed (%s, attempt %d, next in %.0fs): %s",
            kind,
            self._failures,
            self._interval + self._backoff,
            exc,
        )
