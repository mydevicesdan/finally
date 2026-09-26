# Market Data Backend — Detailed Design

**Status:** Design of record for `backend/app/market/`, and **implemented**. The code blocks below are generated from the modules in `backend/app/market/`. Supersedes `planning/archive/MARKET_DATA_DESIGN.md`.
**Audience:** The Backend / Market Data agent implementing or changing the market data subsystem, and the agents building the portfolio, watchlist, chat and frontend layers that consume it.

The market data subsystem already exists (see `MARKET_DATA_SUMMARY.md`). This document is a full design for it: the unified API, the GBM simulator and the Massive API client. It includes every module as working code. Where the design changes the current code, the change is listed in §2 and marked **(change)** in the sections below. All code here runs against `massive==2.2.0`, `numpy>=2` and Python 3.12. The backend test suite (174 tests, 100% coverage) passes.

---

## Table of Contents

1. [Requirements](#1-requirements)
2. [Changes From the Current Implementation](#2-changes-from-the-current-implementation)
3. [Architecture](#3-architecture)
4. [File Layout & Public API](#4-file-layout--public-api)
5. [Ticker Normalization — `tickers.py`](#5-ticker-normalization--tickerspy)
6. [Data Model — `models.py`](#6-data-model--modelspy)
7. [Price Cache — `cache.py`](#7-price-cache--cachepy)
8. [Unified Interface — `interface.py`](#8-unified-interface--interfacepy)
9. [Simulator — `seed_prices.py` + `simulator.py`](#9-simulator)
10. [Massive API Client — `massive_client.py`](#10-massive-api-client)
11. [Factory & Configuration — `factory.py`](#11-factory--configuration)
12. [SSE Streaming — `stream.py`](#12-sse-streaming)
13. [Application Integration](#13-application-integration)
14. [Frontend Contract](#14-frontend-contract)
15. [Testing](#15-testing)
16. [Failure Modes](#16-failure-modes)
17. [Implementation Checklist](#17-implementation-checklist)

---

## 1. Requirements

From `PLAN.md` §6, plus what the consuming features need:

| # | Requirement | Where it's met |
|---|---|---|
| R1 | One abstract interface, two implementations (simulator, Massive), chosen by `MASSIVE_API_KEY` | §8, §9, §10, §11 |
| R2 | Simulator: GBM, ~500 ms ticks, per-ticker drift/volatility, correlated moves, occasional 2–5% jumps, realistic seed prices, in-process, no dependencies | §9 |
| R3 | Massive: REST polling (not WebSocket), one call for all tickers, 15 s default interval (free tier 5 req/min), faster on paid tiers | §10 |
| R4 | Shared in-memory cache holding latest price, previous price, timestamp per ticker | §7 |
| R5 | `GET /api/stream/prices` SSE: ticker, price, previous price, timestamp, direction, ~500 ms cadence, auto-reconnect | §12 |
| R6 | Watchlist shows **daily change %**, not only tick-to-tick change | §6 (`session_open`) |
| R7 | Trades fill instantly at the current price; portfolio valuation and LLM context need prices for every *held* ticker, even ones removed from the watchlist | §13.3, §13.4 |
| R8 | Watchlist changes (REST or LLM) take effect in the price stream without restart | §13.3 |
| R9 | Deterministic, fast unit tests; no network in tests | §15 |

---

## 2. Changes From the Current Implementation

The code that existed before this design worked for the simulator path but had gaps. This design fixes them, and all fixes are implemented. Items 1–3 and 16 affected behaviour users would see.

| # | Severity | Problem in current code | Fix in this design |
|---|---|---|---|
| 1 | **High** | `massive_client.py` reads `snap.last_trade.timestamp`. In `massive` 2.x `LastTrade` has **no** `timestamp` field (it has `sip_timestamp`, in **nanoseconds**). Every snapshot raises `AttributeError` and is skipped, so **the Massive source never produces a price**. The tests don't catch this because they build snapshots with `MagicMock`, which accepts any attribute. (Reproduced: a real `TickerSnapshot.from_dict(...)` leaves the cache empty.) | `parse_snapshot()` uses the real fields and has fallbacks (§10.3). Tests build snapshots with `TickerSnapshot.from_dict()` on wire-shaped JSON (§15.2). |
| 2 | **High** | No reference price for **daily change %** (PLAN §10 watchlist). `PriceUpdate.change_percent` is tick-to-tick, which is always about 0.01%. | Add `session_open` to `PriceUpdate`, plus `day_change` and `day_change_percent` (§6). Massive uses `prev_day.close`. The simulator uses the price at which the ticker entered the simulation. |
| 3 | **High** | If a ticker is removed from the watchlist while shares are still held, it leaves the cache. Portfolio valuation and P&L then lose its price. | The set of tracked tickers is **watchlist ∪ open positions**, kept in sync by `sync_tracked_tickers()` (§13.3). |
| 4 | Medium | `PriceCache.remove()` doesn't bump `version`, and the SSE loop skips empty snapshots. Clients keep showing removed tickers, and removing the last ticker never reaches the client. | `remove()` bumps `version`. SSE sends `data: {}` when the cache is empty (§7, §12). |
| 5 | Medium | Race in Massive: a ticker removed while a poll is in flight gets written back into the cache when the poll returns. | Re-check the tracked set after the fetch (§10.4). |
| 6 | Medium | `RESTClient` is built with default `retries=3`, and urllib3 retries **429s**. One rate-limited poll can use four requests of the 5/min free budget. There's no backoff. | `retries=0`, the poll loop owns retries, with exponential backoff and a longer backoff for auth errors (§10.5). |
| 7 | Medium | A ticker added under Massive has no price for up to 15 s. A trade on it fails in the meantime. | `add_ticker()` wakes the poller early, still respecting rate-limit spacing (§10.4). |
| 8 | Medium | Unknown tickers in the simulator get `random.uniform(50, 300)`. After a restart a held position re-prices to a new random value and P&L jumps by hundreds of %. | Deterministic seed price from a CRC of the symbol (§9.2). |
| 9 | Low | Only Massive normalizes tickers (`upper().strip()`). The simulator doesn't, so `"pypl"` and `"PYPL"` are different tickers there. Nothing validates input. | `normalize_ticker()` is shared by both sources and the API routes (§5). |
| 10 | Low | `stream.py` registers the route on a module-level `router`. Calling `create_stream_router()` twice (tests, app factory) registers `/prices` twice. | Build the router inside the factory (§12). |
| 11 | Low | `PriceCache.version` is read without the lock, so version and data can be torn between two reads. | `snapshot()` returns `(version, prices)` atomically (§7). |
| 12 | Low | `timestamp or time.time()` treats `0.0` as missing. | `time.time() if timestamp is None else timestamp`. |
| 13 | Low | Simulator uses global `np.random` / `random`, so tests can't be deterministic. `add_ticker()` before `start()` is silently dropped. A second `start()` leaks a task. | Per-instance `np.random.Generator(seed)`. The simulator is created eagerly. A second `start()` raises `RuntimeError`. |
| 14 | Low | No SSE keep-alive. An idle stream (empty watchlist) can be closed by proxies. | Comment heartbeat every 15 s (§12). |
| 15 | Low | `np.linalg.cholesky` failure (a bad edit to correlation constants) would crash the simulator. | Fall back to independent moves and log a warning (§9.3). |
| 16 | **High** | `get_snapshot_all(market_type=SnapshotMarketType.STOCKS)`. In `massive` 2.x, `SnapshotMarketType` is a plain `Enum`. The SDK formats the member itself into the URL, which gives `/v2/snapshot/locale/global/markets/SnapshotMarketType.STOCKS/tickers`. That endpoint doesn't exist, so every request fails. Found during implementation. | Pass `SnapshotMarketType.STOCKS.value` (`"stocks"`). A regression test drives the real `RESTClient` down to `_get` and asserts the path (§10.4). |

`MASSIVE_API.md` (archive) is also inaccurate for the installed SDK. §10.2 is the corrected field reference.

---

## 3. Architecture

```
                         ┌──────────────────────────────────────┐
  MASSIVE_API_KEY? ──►   │ create_market_data_source(cache)      │
                         └──────────────┬───────────────────────┘
                     empty/unset        │         set
              ┌─────────────────────────┴─────────────────────────┐
              ▼                                                   ▼
  ┌───────────────────────┐                        ┌────────────────────────────┐
  │ SimulatorDataSource    │                        │ MassiveDataSource           │
  │  asyncio task, 500 ms  │                        │  asyncio task, 15 s (cfg)   │
  │  GBMSimulator.step()   │                        │  to_thread(get_snapshot_all)│
  └──────────┬────────────┘                        └─────────────┬──────────────┘
             │ cache.update(ticker, price, ts, session_open)      │
             └────────────────────────┬───────────────────────────┘
                                      ▼
                      ┌───────────────────────────────┐
                      │ PriceCache  (Lock, version++)  │  ◄── single source of truth
                      └───┬──────────┬──────────┬─────┘
                          │          │          │
            snapshot()    │  get()   │   get()  │  get_all()
                          ▼          ▼          ▼
               SSE /api/stream   trade      portfolio valuation,
               /prices (per      execution  snapshots, LLM context
               client, 500 ms)

  Watchlist / trade routes ──► sync_tracked_tickers(source, watchlist ∪ positions)
                               └─► source.add_ticker() / source.remove_ticker()
```

**Principles**

- **Push into a cache; don't pull from the source.** Sources write on their own schedule and consumers read the cache. No consumer knows or cares which source is active. Consumers never block on a network call.
- **One writer, many readers.** Only the active source writes prices. `remove()` is called only through the source, which keeps source state and cache consistent.
- **Full snapshots on the wire.** Every SSE `data:` frame carries every tracked ticker. The client state is always "replace the map", with no diff bookkeeping and no stale tickers after a reconnect.
- **Failures stay inside the source.** A background source logs, backs off and retries. It never raises into the app. Missing prices appear as `None` from the cache, and routes turn that into a clear HTTP error.

**Concurrency model**

| Code | Runs on | Touches |
|---|---|---|
| `SimulatorDataSource._run_loop` | event loop | `GBMSimulator` state, `PriceCache` |
| `MassiveDataSource._fetch_snapshots` | worker thread (`asyncio.to_thread`) | HTTP only, no shared state |
| `MassiveDataSource._poll_once` (rest) | event loop | `_tickers`, `PriceCache` |
| SSE generators, API routes | event loop | `PriceCache` (read), source add/remove |

All mutation of source state happens on the event loop, so it needs no locks. `PriceCache` has a `threading.Lock` because it's the shared boundary. It stays safe if a future source writes from a thread, and on free-threaded Python.

---

## 4. File Layout & Public API

```
backend/app/market/
  __init__.py         Re-exports the public API below
  tickers.py          normalize_ticker(), normalize_tickers()          (new)
  models.py           PriceUpdate                                      (change: session_open)
  cache.py            PriceCache                                       (change: snapshot(), remove bumps version)
  interface.py        MarketDataSource ABC                             (change: stricter contract)
  seed_prices.py      SEED_PRICES, TICKER_PARAMS, correlation constants (unchanged)
  simulator.py        seed_price_for(), GBMSimulator, SimulatorDataSource
  massive_client.py   parse_snapshot(), classify_error(), MassiveDataSource
  factory.py          create_market_data_source()
  stream.py           create_stream_router(), generate_price_events()
  sync.py             sync_tracked_tickers(source, desired)              (new)
```

**`app/market/__init__.py`**

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate               - Immutable price snapshot dataclass
    PriceCache                - Thread-safe in-memory price store
    MarketDataSource          - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router      - FastAPI router factory for the SSE endpoint
    normalize_ticker          - Canonical, validated ticker symbol
    sync_tracked_tickers      - Make a source track exactly a given ticker set
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router
from .sync import sync_tracked_tickers
from .tickers import normalize_ticker

__all__ = [
    "MarketDataSource",
    "PriceCache",
    "PriceUpdate",
    "create_market_data_source",
    "create_stream_router",
    "normalize_ticker",
    "sync_tracked_tickers",
]
```

Downstream code imports only from `app.market`. Everything else is internal.

---

## 5. Ticker Normalization — `tickers.py`

One function decides what a ticker looks like, and every entry point uses it: both sources, `POST /api/watchlist`, `POST /api/portfolio/trade`, and LLM-requested trades or watchlist changes. That keeps `"pypl "`, `"PYPL"` and `"Pypl"` from becoming three tickers, and it rejects junk before it reaches the database or the Massive query string.

```python
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
```

API routes turn `ValueError` into HTTP 400 (§13.3). The regex only checks shape. Whether a symbol exists is known only to the source: the simulator accepts any valid-looking symbol, and Massive returns no data for unknown ones (§10.4).

---

## 6. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market data layer. It's immutable (`frozen=True, slots=True`), so the same instance can be shared across threads and SSE clients without copying.

```python
"""Data models for market data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Direction = Literal["up", "down", "flat"]


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time.

    `previous_price` is the price from the previous update (tick-to-tick, drives
    the green/red flash). `session_open` is the reference price for the daily
    change shown in the watchlist: the previous close for Massive, the price at
    which the ticker entered the simulation for the simulator.
    """

    ticker: str
    price: float
    previous_price: float
    timestamp: float  # Unix seconds
    session_open: float

    @property
    def change(self) -> float:
        """Absolute change since the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percent change since the previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> Direction:
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change(self) -> float:
        """Absolute change since the session reference price."""
        return round(self.price - self.session_open, 4)

    @property
    def day_change_percent(self) -> float:
        """Percent change since the session reference price."""
        if self.session_open == 0:
            return 0.0
        return round((self.price - self.session_open) / self.session_open * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "session_open": self.session_open,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

**Two kinds of change**

| Field | Reference | Used for |
|---|---|---|
| `previous_price`, `change`, `change_percent`, `direction` | previous update | green/red flash, tick arrow |
| `session_open`, `day_change`, `day_change_percent` | session reference | watchlist "daily change %" column, LLM context |

**`session_open` per source**

- **Massive:** `prev_day.close` from the snapshot, which is the standard definition of daily change. It matches Massive's own `todaysChangePerc`.
- **Simulator:** the price when the ticker entered the simulation (app start, or when it was added). "Daily" therefore means "since this simulated session began". That's the honest definition, since the simulator has no calendar.

`to_dict()` output (one ticker):

```json
{
  "ticker": "AAPL",
  "price": 190.42,
  "previous_price": 190.38,
  "timestamp": 1727366399.123,
  "change": 0.04,
  "change_percent": 0.021,
  "direction": "up",
  "session_open": 189.17,
  "day_change": 1.25,
  "day_change_percent": 0.6608
}
```

The existing fields keep their names and meaning. `session_open`, `day_change` and `day_change_percent` are additions, so existing consumers aren't broken.

---

## 7. Price Cache — `cache.py`

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker, shared by one writer and many readers.

    Writers: the active MarketDataSource (event loop or a worker thread).
    Readers: SSE stream, portfolio valuation, trade execution, LLM context.

    Every mutation (update *and* remove) bumps `version`, so the SSE stream
    can cheaply tell whether anything changed since its last push.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        session_open: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        - First update for a ticker: previous_price == price (direction "flat").
        - session_open: explicit value wins; otherwise the previous one is kept;
          for a brand-new ticker it defaults to the first price.
        """
        price = round(price, 2)
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if session_open is None:
                session_open = prev.session_open if prev else price
            update = PriceUpdate(
                ticker=ticker,
                price=price,
                previous_price=previous_price,
                timestamp=time.time() if timestamp is None else timestamp,
                session_open=round(session_open, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)

    def snapshot(self) -> tuple[int, dict[str, PriceUpdate]]:
        """Atomically read (version, all prices) — used by the SSE stream."""
        with self._lock:
            return self._version, dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: object) -> bool:
        with self._lock:
            return ticker in self._prices
```

**Design notes**

- **Rounding happens once, at the cache boundary** (cents). The simulator keeps unrounded internal state so rounding error never compounds, and everything downstream (trades, P&L, SSE) sees the same two-decimal price.
- **`version`** is a global monotonic counter. It works like an ETag for the SSE stream: one integer comparison per client per 500 ms tells the stream whether to serialize anything. A per-ticker "dirty set" would be more precise, but it would need per-client state, and full snapshots of about 10–50 tickers are only a few KB.
- **`snapshot()`** reads version and data under one lock acquisition. Reading `version` then `get_all()` could pair an old version with new data. That's harmless today but trivially avoided.
- **Memory** is O(tickers): only the latest update is kept. Price history for charts is built on the frontend from the stream (PLAN §2). Portfolio history is stored in `portfolio_snapshots`, not here.

---

## 8. Unified Interface — `interface.py`

```python
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
```

**Why the source writes to the cache instead of returning prices.** The two sources have very different natural cadences (500 ms vs 15 s) and failure modes (CPU-only vs network). With push-to-cache, readers get O(1) non-blocking access, stay unaffected by source latency or outages, and are written once for both sources. The interface covers only the lifecycle and the ticker set, which is all that differs between sources.

**Lifecycle**

```python
cache = PriceCache()
source = create_market_data_source(cache)      # unstarted
await source.add_ticker("PYPL")                # allowed before start
await source.start(["AAPL", "GOOGL", "MSFT"])  # cache primed before this returns
cache.get("AAPL")                              # PriceUpdate, never None after start (simulator)
await source.remove_ticker("GOOGL")            # gone from cache immediately
await source.stop()                            # idempotent
```

---

## 9. Simulator

### 9.1 Model

Each tick advances every ticker with geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt + σ·√dt·Z )
```

| Symbol | Meaning | Value |
|---|---|---|
| μ | annualized drift | 0.03–0.08 per ticker |
| σ | annualized volatility | 0.17 (V) – 0.50 (TSLA) |
| dt | one tick as a fraction of a trading year | 0.5 s / (252 · 6.5 · 3600 s) ≈ 8.48 × 10⁻⁸ |
| Z | correlated standard normal | `L @ z`, where `L = cholesky(C)` |

**What the numbers mean on screen** (checked by the calibration test in §15.3):

| Horizon | AAPL (σ = 0.22, $190) | TSLA (σ = 0.50, $250) |
|---|---|---|
| 1 tick (500 ms) | σ·√dt ≈ 0.0064% ≈ **$0.012** | 0.0146% ≈ **$0.036** |
| 1 minute (120 ticks) | ≈ 0.07% ≈ $0.13 | ≈ 0.16% ≈ $0.40 |
| 6.5 h of wall-clock | ≈ 1.4% (a realistic trading day) | ≈ 3.1% |

So one simulated wall-clock second equals one real market second. At cent resolution, low-priced, low-volatility tickers print "flat" on some ticks, as real tapes do. **Jumps** add drama: each ticker has a `p = 0.001` chance per tick of a ±2–5% move. With 10 tickers at 2 ticks/s that's one jump somewhere about every 50 s.

GBM is multiplicative, so prices can never reach zero or go negative. Using `exp(...)` rather than `1 + μdt + σ√dt·Z` keeps it exact for any dt.

### 9.2 Parameters — `seed_prices.py` (unchanged)

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

**Correlation structure** (pairwise ρ used to build `C`):

| Pair | ρ |
|---|---|
| tech × tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| JPM × V | 0.5 |
| TSLA × anything | 0.3 (TSLA is in the tech set but overridden first) |
| everything else, including unknown tickers | 0.3 |

This matrix is positive definite for any mix of these groups and any number of unknown tickers, so Cholesky always succeeds. The test in §15.3 uses 50 tickers. Unknown tickers get `DEFAULT_PARAMS` (σ 0.25, μ 0.05) and a **deterministic** seed price in [$50, $300) from `crc32(symbol)`. That makes `PYPL` start at the same price after every restart, so a held position's P&L stays meaningful.

To make a new ticker realistic, add it to `SEED_PRICES`, `TICKER_PARAMS` and, optionally, a `CORRELATION_GROUPS` set. No code changes are needed.

### 9.3 `simulator.py`

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import zlib

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)
from .tickers import normalize_ticker, normalize_tickers

logger = logging.getLogger(__name__)


def seed_price_for(ticker: str) -> float:
    """Starting price for a ticker.

    Known tickers use SEED_PRICES. Unknown tickers get a price in [50, 300)
    derived from a CRC of the symbol, so "PYPL" starts at the same price on
    every restart (a random price would turn a held position's P&L into noise
    each time the container restarts).
    """
    if ticker in SEED_PRICES:
        return SEED_PRICES[ticker]
    return round(50.0 + (zlib.crc32(ticker.encode()) % 25_000) / 100.0, 2)


class GBMSimulator:
    """Correlated Geometric Brownian Motion for a dynamic set of tickers.

        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Z is a vector of standard normals correlated via the Cholesky factor of a
    sector-based correlation matrix. Pure and synchronous: no asyncio, no cache.
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # ~8.48e-8 (one 500ms tick)

    def __init__(
        self,
        tickers: list[str] | None = None,
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._dt = dt
        self._sqrt_dt = math.sqrt(dt)
        self._event_prob = event_probability
        self._rng = np.random.default_rng(seed)
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}  # unrounded internal state
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        self.add_tickers(tickers or [])

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Returns {ticker: price rounded to cents}."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = self._rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z
        shocks = self._rng.random(n) < self._event_prob

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]
            drift = (mu - 0.5 * sigma * sigma) * self._dt
            diffusion = sigma * self._sqrt_dt * z[i]
            price = self._prices[ticker] * math.exp(drift + diffusion)

            if shocks[i]:  # rare 2-5% jump for visual drama
                magnitude = self._rng.uniform(0.02, 0.05)
                sign = 1.0 if self._rng.random() < 0.5 else -1.0
                price *= 1.0 + sign * magnitude
                logger.debug("Shock on %s: %+.1f%%", ticker, sign * magnitude * 100)

            self._prices[ticker] = price
            result[ticker] = round(price, 2)
        return result

    def add_tickers(self, tickers: list[str]) -> None:
        """Add several tickers with a single correlation-matrix rebuild."""
        added = False
        for ticker in tickers:
            if ticker in self._prices:
                continue
            self._tickers.append(ticker)
            self._prices[ticker] = seed_price_for(ticker)
            self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))
            added = True
        if added:
            self._rebuild_cholesky()

    def add_ticker(self, ticker: str) -> None:
        self.add_tickers([ticker])

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        price = self._prices.get(ticker)
        return round(price, 2) if price is not None else None

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        try:
            self._cholesky = np.linalg.cholesky(corr)
        except np.linalg.LinAlgError:
            # Cannot happen with the shipped constants, but a bad edit to
            # seed_prices.py must degrade to uncorrelated moves, not crash.
            logger.warning("Correlation matrix not positive definite; using independent moves")
            self._cholesky = None

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource that steps a GBMSimulator every `update_interval` seconds."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        # Created eagerly so add_ticker() works before start().
        self._sim = GBMSimulator(event_probability=event_probability, seed=seed)
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("SimulatorDataSource already started")
        self._sim.add_tickers(normalize_tickers(tickers))
        for ticker in self._sim.get_tickers():
            self._seed_cache(ticker)  # SSE has data before the first tick
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(self._sim.get_tickers()))

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
        if ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._seed_cache(ticker)  # tradeable immediately, no waiting for a tick
        logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers()

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker)
        if price is not None:
            self._cache.update(ticker, price, session_open=price)

    async def _run_loop(self) -> None:
        while True:
            try:
                # step() and the cache writes run without an await in between,
                # so add/remove (also on the event loop) can never interleave.
                for ticker, price in self._sim.step().items():
                    self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

**Notes**

- **`GBMSimulator` is pure and synchronous.** It has no asyncio and no cache, so tests can step it thousands of times in milliseconds with a fixed `seed`.
- **Vectorised random draws** (`standard_normal(n)`, `random(n)`) happen once per tick. The per-ticker loop is a handful of float ops. 50 tickers × 2 ticks/s is negligible CPU.
- **Cholesky rebuild** on add/remove is O(n³) with n < 50: microseconds. `add_tickers()` batches the start-up set into one rebuild.
- **Atomicity:** `step()` and the cache writes have no `await` between them, so `add_ticker`/`remove_ticker` (also on the event loop) can't interleave. A removed ticker is never written back.
- **Drift over long sessions:** with μ ≈ 5%/yr, a full simulated day adds about 0.02%. Drift is there for correctness, not effect.

---

## 10. Massive API Client

### 10.1 Endpoint & plans

| Item | Value |
|---|---|
| Package | `massive` (formerly `polygon-api-client`); locked at 2.2.0 |
| Base URL | `https://api.massive.com` (legacy `api.polygon.io` still served) |
| Endpoint | `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,…` |
| SDK call | `RESTClient.get_snapshot_all(market_type=SnapshotMarketType.STOCKS.value, tickers=[...])`. Pass the **string** value; the enum member itself builds a wrong URL in 2.x. |
| Cost | **One request for all tickers**, so the request rate doesn't depend on watchlist size |
| Free tier | 5 requests/min → poll every **15 s** (minimum spacing 12 s) |
| Paid tiers | effectively unlimited; poll every 2–5 s (`MASSIVE_POLL_INTERVAL`) |

> Snapshot endpoints are not included in every Massive plan. If the key's plan lacks them, the API answers `NOT_AUTHORIZED`. The poller logs an actionable error once, then retries every ~2 minutes. The fix is to upgrade the plan or unset `MASSIVE_API_KEY` to use the simulator (§16).

### 10.2 Response → SDK field mapping (verified against `massive` 2.2.0)

Wire JSON (one element of the response's `tickers` array) and the SDK attribute each key becomes:

```json
{
  "ticker": "AAPL",                           // snap.ticker
  "todaysChange": 1.25,                       // snap.todays_change
  "todaysChangePerc": 0.66,                   // snap.todays_change_percent
  "updated": 1727366399123456789,             // snap.updated              (ns)
  "day":     {"o":189.1,"h":191.2,"l":188.7,"c":190.4,"v":48211234,"vw":190.02},
                                              // snap.day: Agg(open, high, low, close, volume, vwap)
  "min":     {"c":190.4,"t":1727366340000,…}, // snap.min: MinuteSnapshot(close, timestamp (ms), …)
  "prevDay": {"c":189.17,…},                  // snap.prev_day: Agg(close=previous close)
  "lastTrade": {"p":190.42,"s":100,"t":1727366399123456789,…},
                                              // snap.last_trade: LastTrade(price, size, sip_timestamp (ns))
  "lastQuote": {"P":190.43,"p":190.41,…}      // snap.last_quote
}
```

| We need | Read from | Units / notes |
|---|---|---|
| price | `last_trade.price` → `min.close` → `day.close` → `prev_day.close` | first non-empty wins. Fallbacks cover pre-market, weekends and illiquid names. |
| timestamp | `last_trade.sip_timestamp` (→ `participant_timestamp`) | **nanoseconds** → `/ 1e9` |
| | else `min.timestamp` | **milliseconds** → `/ 1e3` |
| | else `snap.updated` | **nanoseconds** → `/ 1e9`; else `time.time()` |
| session_open | `prev_day.close` | may be absent for new listings → cache keeps or defaults it |

There is **no** `last_trade.timestamp`, `day.previous_close` or `day.change_percent` attribute in the SDK, despite what the archived `MASSIVE_API.md` shows.

### 10.3 Parsing is a pure function

Parsing is kept separate from I/O. `parse_snapshot()` takes a real `TickerSnapshot` and returns a small `ParsedQuote` or `None`. It can be unit-tested with `TickerSnapshot.from_dict(wire_json)`, which is exactly what the SDK does with a live response (§15.2).

### 10.4 `massive_client.py`

```python
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
```

### 10.5 Poll scheduling, rate limits and backoff

```
           ┌────────── wait up to (interval + backoff) ──────────┐
 poll ─────┤  or until add_ticker() sets _wake                    ├──► enforce spacing ──► poll
           └──────────────────────────────────────────────────────┘    max(min_spacing, backoff)
                                                                        since last request
```

| Situation | Next request after |
|---|---|
| normal | `interval` (15 s default) |
| `add_ticker()` | as soon as `min_spacing` (12 s free tier, or `interval` if smaller) has passed since the last request |
| 429 / network / 5xx, n-th consecutive | `interval + min(120, interval · 2^(n−1))` → 30 s, 45 s, 75 s, 135 s… |
| bad key / plan lacks snapshots | `interval + 120 s`. Logged as ERROR once, not every cycle. |
| any success | backoff reset |

Worst-case request rate is therefore ≤ 1 / 12 s = 5/min with the defaults. `min_poll_spacing = min(poll_interval, 12)` means a paid-tier user who sets `MASSIVE_POLL_INTERVAL=2` gets 2 s spacing, and nobody else can accidentally exceed the free limit.

**Why `asyncio.to_thread`:** `RESTClient` is synchronous (urllib3). A 10 s read timeout on the event loop would freeze every SSE stream and API route. The worker thread only does HTTP. Parsing and cache writes happen back on the loop.

### 10.6 Market hours

The snapshot keeps returning the last trade outside market hours, so prices stay put and each poll reports `direction: "flat"`. That's correct and needs no special handling. The frontend shows a still market. `timestamp` stays the time of the last trade, which is accurate. Pre-market, `day` may still hold the previous session. `prev_day.close` remains the correct daily-change reference.

---

## 11. Factory & Configuration

### 11.1 `factory.py`

```python
"""Select the market data source from the environment."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _float_env(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default
    return value if value > 0 else default


def create_market_data_source(
    price_cache: PriceCache,
    env: Mapping[str, str] | None = None,
) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty -> Massive; otherwise the simulator.

    Returns an unstarted source; the caller awaits source.start(tickers).
    """
    env = os.environ if env is None else env
    api_key = env.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _float_env(env, "MASSIVE_POLL_INTERVAL", 15.0)
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

`env` is injectable so tests never mutate `os.environ`.

### 11.2 Configuration

| Setting | Where | Default | Notes |
|---|---|---|---|
| `MASSIVE_API_KEY` | env | unset | set + non-empty → Massive; otherwise simulator (PLAN §5) |
| `MASSIVE_POLL_INTERVAL` | env | `15` | seconds; optional. Set 2–5 on paid plans. |
| `update_interval` | `SimulatorDataSource(...)` | `0.5` s | simulator tick |
| `event_probability` | `SimulatorDataSource(...)` | `0.001` | jump chance per ticker per tick |
| `seed` | `SimulatorDataSource(...)` | `None` | fixed seed for deterministic runs and tests |
| `min_poll_spacing` | `MassiveDataSource(...)` | `min(interval, 12)` | rate-limit guard |
| `interval` / `heartbeat` | `create_stream_router(...)` | `0.5` s / `15` s | SSE cadence / keep-alive |
| SSE `retry:` | `generate_price_events` | `1000` ms | browser reconnect delay |

Add `MASSIVE_POLL_INTERVAL=` (empty) to `.env.example` under `MASSIVE_API_KEY`.

---

## 12. SSE Streaming

### 12.1 `stream.py`

```python
"""SSE endpoint streaming live prices from the PriceCache."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    """Build a fresh router (no module-level state) serving GET /api/stream/prices."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames: a full snapshot whenever the cache version changes.

    Every `data:` frame is the complete set of tracked tickers, so the client
    simply replaces its map (removed tickers disappear, empty dict = empty
    watchlist). A comment frame keeps idle connections alive through proxies.
    """
    yield "retry: 1000\n\n"
    last_version = -1
    last_sent = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version, prices = price_cache.snapshot()
            now = time.monotonic()
            if version != last_version:
                last_version = version
                payload = {ticker: update.to_dict() for ticker, update in prices.items()}
                yield f"data: {json.dumps(payload)}\n\n"
                last_sent = now
            elif now - last_sent >= heartbeat:
                yield ": keep-alive\n\n"
                last_sent = now
            await asyncio.sleep(interval)
    finally:
        logger.info("SSE client disconnected: %s", client)
```

### 12.2 Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.42,...},"GOOGL":{...}, ...}

data: {"AAPL":{...}, ...}

: keep-alive

data: {}
```

- `retry: 1000` is sent first and tells `EventSource` to reconnect 1 s after a drop.
- `data:` is the **complete** map of tracked tickers, sent at most every 500 ms and only when the cache changed. After a reconnect the first frame restores the full state.
- `: keep-alive` is a comment, ignored by `EventSource`. It's sent after 15 s without data. With the simulator it's rarely needed. Under Massive (15 s polls) or with an empty watchlist it stops idle-timeouts at proxies and load balancers.
- `data: {}` means no tickers are tracked.
- The default event type is used (no `event:` line), so the client uses `onmessage`.

### 12.3 Why poll-and-push per client

Each client's generator checks `version` every 500 ms. The alternative is a broadcast queue per client, fed by the source. That would push each tick sooner (≤ 500 ms sooner) at the cost of queue lifecycle management, back-pressure handling for slow clients, and cleanup on disconnect. With one user and a 500 ms source cadence, the version check is simpler and just as smooth. The cost is one lock acquisition per client per 500 ms. If multi-user fan-out ever matters, swap in an `asyncio.Condition` on the cache without touching the wire format.

### 12.4 Bandwidth

About 250 bytes per ticker per frame. Ten tickers at 2 frames/s is about 5 KB/s. Fifty tickers is about 25 KB/s. That's fine on localhost and on a cloud deployment.

---

## 13. Application Integration

The code in this section is for the app layer (`backend/app/main.py`, routers, DB). The DB helpers (`db.get_watchlist_tickers()` and similar) belong to the database layer and are named here only to show the call sites.

### 13.1 Startup and shutdown (`main.py`)

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app import db
from app.market import PriceCache, create_market_data_source, create_stream_router
from app.market_sync import desired_tickers


def create_app() -> FastAPI:
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init()                                    # lazy schema + seed (PLAN §7)
        await source.start(sorted(desired_tickers()))  # watchlist ∪ held positions
        try:
            yield
        finally:
            await source.stop()

    app = FastAPI(title="FinAlly", lifespan=lifespan)
    app.state.price_cache = price_cache
    app.state.market_source = source

    app.include_router(create_stream_router(price_cache))
    # app.include_router(portfolio_router); app.include_router(watchlist_router); ...

    # Static frontend last, so /api/* routes win.
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
    return app


app = create_app()
```

The cache and source are built in `create_app()`, before the lifespan runs. The stream router needs the cache when routes are registered. `start()` runs inside the lifespan, so the background task lives on uvicorn's event loop.

### 13.2 Dependencies (`app/deps.py`)

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

### 13.3 Keeping the tracked set right — `app/market/sync.py`

The tracked set is **watchlist ∪ tickers with an open position**. Deriving it from the DB and reconciling after every change is simpler and more robust than adding and removing incrementally in each route. The market package provides the DB-agnostic reconciler:

```python
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
```

The app layer supplies the desired set from the database (`app/market_sync.py`):

```python
from app import db
from app.market import MarketDataSource, sync_tracked_tickers


def desired_tickers(user_id: str = "default") -> set[str]:
    return set(db.get_watchlist_tickers(user_id)) | set(db.get_position_tickers(user_id))


async def sync_market(source: MarketDataSource, user_id: str = "default") -> None:
    await sync_tracked_tickers(source, desired_tickers(user_id))
```

Call `sync_market()` after watchlist add/remove, after every trade, and after the chat flow runs LLM actions. It's O(tickers) with no I/O beyond two small queries.

**Watchlist routes**

```python
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app import db
from app.deps import get_market_source, get_price_cache
from app.market import MarketDataSource, PriceCache, normalize_ticker
from app.market_sync import sync_market

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


class WatchlistAdd(BaseModel):
    ticker: str


@router.get("")
async def list_watchlist(cache: PriceCache = Depends(get_price_cache)):
    items = []
    for ticker in db.get_watchlist_tickers():
        update = cache.get(ticker)
        items.append({"ticker": ticker, **(update.to_dict() if update else {"price": None})})
    return items


@router.post("", status_code=201)
async def add_to_watchlist(
    body: WatchlistAdd,
    source: MarketDataSource = Depends(get_market_source),
    cache: PriceCache = Depends(get_price_cache),
):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    db.add_watchlist_ticker(ticker)           # idempotent (UNIQUE user_id, ticker)
    await sync_market(source)
    update = cache.get(ticker)                # simulator: already there; Massive: maybe next poll
    return {"ticker": ticker, "price": update.price if update else None}


@router.delete("/{ticker}", status_code=204)
async def remove_from_watchlist(ticker: str, source: MarketDataSource = Depends(get_market_source)):
    try:
        ticker = normalize_ticker(ticker)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    db.remove_watchlist_ticker(ticker)
    await sync_market(source)        # still tracked if a position is open
```

### 13.4 Consumers

**Trade execution: price lookup**

```python
async def current_price_for_trade(
    ticker: str, source: MarketDataSource, cache: PriceCache
) -> float:
    """Price to fill a market order at. Starts tracking the ticker if needed."""
    price = cache.get_price(ticker)
    if price is None and ticker not in source.get_tickers():
        await source.add_ticker(ticker)       # simulator: priced instantly
        price = cache.get_price(ticker)
    if price is None:                         # Massive: not polled yet / unknown symbol
        raise HTTPException(
            400, f"No price available for {ticker} yet. Try again in a few seconds."
        )
    return price
```

After the trade (success **or** failure), call `sync_market(source)`. A newly held ticker stays tracked. A ticker added only to price a failed trade is dropped again. A position sold to zero on a non-watchlist ticker is dropped.

**Portfolio valuation (`GET /api/portfolio`, snapshots, LLM context)**

```python
def value_positions(positions: list[dict], cache: PriceCache) -> dict:
    prices = cache.get_all()                  # one lock, consistent view
    rows, total = [], 0.0
    for p in positions:
        update = prices.get(p["ticker"])
        price = update.price if update else p["avg_cost"]   # fall back to cost, never crash
        market_value = p["quantity"] * price
        total += market_value
        rows.append({
            **p,
            "current_price": price,
            "market_value": round(market_value, 2),
            "unrealized_pnl": round((price - p["avg_cost"]) * p["quantity"], 2),
            "pnl_percent": round((price / p["avg_cost"] - 1) * 100, 2) if p["avg_cost"] else 0.0,
            "price_stale": update is None,
        })
    return {"positions": rows, "positions_value": round(total, 2)}
```

Falling back to `avg_cost` covers the few seconds after a Massive startup before the first poll lands. `price_stale` lets the UI show it.

**LLM context line per watched ticker**

```python
u = cache.get(ticker)
line = f"{ticker}: ${u.price:.2f} ({u.day_change_percent:+.2f}% today)" if u else f"{ticker}: n/a"
```

---

## 14. Frontend Contract

```ts
// types/market.ts
export type Direction = "up" | "down" | "flat";

export interface PriceUpdate {
  ticker: string;
  price: number;
  previous_price: number;
  timestamp: number;          // Unix seconds (float)
  change: number;             // vs previous update
  change_percent: number;
  direction: Direction;
  session_open: number;
  day_change: number;         // vs session_open
  day_change_percent: number; // watchlist "daily change %"
}

export type PriceSnapshot = Record<string, PriceUpdate>;
```

```ts
// hooks/usePriceStream.ts (sketch)
const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");              // green dot
es.onerror = () =>
  setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const snapshot: PriceSnapshot = JSON.parse(e.data);
  setPrices(snapshot);                                  // replace, don't merge
  for (const u of Object.values(snapshot)) {
    if (u.direction !== "flat") flash(u.ticker, u.direction);
    appendSparklinePoint(u.ticker, u.timestamp, u.price); // skip if timestamp unchanged
  }
};
```

- **Replace, don't merge.** Every frame is the full set, so removed tickers disappear on their own.
- **Sparklines:** under Massive the same price and timestamp can repeat when the market is closed. Only append when `timestamp` changes.
- **Flash:** `direction` is relative to the previous *cache* update, not the previous frame. With the simulator those are the same. Under Massive each poll produces one update, so each flash shows the move since the last poll.

---

## 15. Testing

Tests live in `backend/tests/market/`. Run them with `uv run --extra dev pytest`. `asyncio_mode = "auto"` is already set in `pyproject.toml`. No test touches the network: Massive tests inject a fake `client=`.

### 15.1 Test plan

| Module | Tests |
|---|---|
| `tickers.py` | normalization (`" brk.b " → "BRK.B"`), rejects empty/numeric/injection/too-long |
| `models.py` | `change`, `direction`, `day_change_percent`, zero-division guards, `to_dict` keys |
| `cache.py` | first update flat, `session_open` explicit/kept/defaulted, `remove` bumps version only when something is removed, `timestamp=0.0` respected, `snapshot()` consistency, concurrent writers (threads) keep `version == number of updates` |
| `simulator.py` | determinism with seed, positive prices over 10k steps, per-tick σ within 5% of `σ√dt`, empirical correlation ≈ 0.6 / 0.3, Cholesky OK for 50 tickers, stable unknown-ticker seed, add before start, double start raises, remove never re-inserted |
| `massive_client.py` | `parse_snapshot` on **wire-shaped JSON via `TickerSnapshot.from_dict`**, fallbacks, `None` when no price, poll writes cache incl. `session_open`, removal during in-flight fetch, early poll on `add_ticker`, backoff progression, `classify_error` |
| `factory.py` | env selection, whitespace-only key → simulator, `MASSIVE_POLL_INTERVAL` parsing and invalid values |
| `stream.py` | first frame `retry:`, snapshot frame JSON, heartbeat, `data: {}` after last removal, stops on disconnect, calling `create_stream_router` twice gives two independent routers |

### 15.2 Massive tests that exercise the real SDK models

This is the test that would have caught change #1:

```python
import asyncio
import time
from unittest.mock import MagicMock

import pytest
from massive.exceptions import BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, classify_error, parse_snapshot

# One element of the snapshot response's "tickers" array, exactly as on the wire.
AAPL_JSON = {
    "ticker": "AAPL",
    "todaysChange": 1.25,
    "todaysChangePerc": 0.66,
    "updated": 1727366399123456789,
    "day": {"o": 189.1, "h": 191.2, "l": 188.7, "c": 190.4, "v": 48211234, "vw": 190.02},
    "min": {"av": 48211234, "o": 190.3, "h": 190.5, "l": 190.2, "c": 190.4,
            "v": 12000, "vw": 190.35, "t": 1727366340000, "n": 120},
    "prevDay": {"o": 187.5, "h": 189.6, "l": 187.1, "c": 189.17, "v": 51000000, "vw": 188.9},
    "lastTrade": {"p": 190.42, "s": 100, "x": 4, "t": 1727366399123456789,
                  "c": [14, 41], "i": "52983525029461"},
    "lastQuote": {"P": 190.43, "S": 2, "p": 190.41, "s": 3, "t": 1727366399223456789},
}


class TestParseSnapshot:
    def test_parses_real_wire_shape(self):
        q = parse_snapshot(TickerSnapshot.from_dict(AAPL_JSON))
        assert q.ticker == "AAPL"
        assert q.price == 190.42
        assert q.timestamp == pytest.approx(1727366399.123456789)   # ns -> s
        assert q.session_open == 189.17

    def test_falls_back_to_prev_close_without_trades(self):
        data = {"ticker": "AAPL", "prevDay": {"c": 189.17}, "updated": 1727366399000000000}
        q = parse_snapshot(TickerSnapshot.from_dict(data))
        assert q.price == 189.17
        assert q.timestamp == pytest.approx(1727366399.0)

    def test_no_price_anywhere_returns_none(self):
        assert parse_snapshot(TickerSnapshot.from_dict({"ticker": "ZZZZ"})) is None


class TestMassiveSource:
    async def test_poll_writes_cache(self):
        cache = PriceCache()
        client = MagicMock()
        client.get_snapshot_all.return_value = [TickerSnapshot.from_dict(AAPL_JSON)]
        source = MassiveDataSource("k", cache, client=client)
        await source.start(["aapl"])                  # normalized to AAPL
        u = cache.get("AAPL")
        assert u.price == 190.42 and u.session_open == 189.17
        assert round(u.day_change_percent, 2) == 0.66  # matches Massive's todaysChangePerc
        await source.stop()

    async def test_removed_during_fetch_not_reinserted(self):
        cache = PriceCache()
        source = MassiveDataSource("k", cache, client=MagicMock())
        source._tickers = ["AAPL"]

        def slow_fetch(tickers):
            time.sleep(0.05)                           # runs in the worker thread
            return [TickerSnapshot.from_dict(AAPL_JSON)]

        source._fetch_snapshots = slow_fetch
        poll = asyncio.create_task(source._poll_once())
        await asyncio.sleep(0.01)
        await source.remove_ticker("AAPL")
        await poll
        assert "AAPL" not in cache

    async def test_add_ticker_polls_early(self):
        cache = PriceCache()
        client = MagicMock()
        client.get_snapshot_all.return_value = []
        source = MassiveDataSource("k", cache, poll_interval=60, min_poll_spacing=0, client=client)
        await source.start(["MSFT"])
        client.get_snapshot_all.return_value = [TickerSnapshot.from_dict(AAPL_JSON)]
        await source.add_ticker("AAPL")
        await asyncio.sleep(0.1)                       # far less than the 60 s interval
        assert cache.get_price("AAPL") == 190.42
        await source.stop()

    async def test_failure_backs_off(self):
        client = MagicMock()
        client.get_snapshot_all.side_effect = BadResponse(
            '{"status":"ERROR","error":"You\'ve exceeded the maximum requests per minute"}'
        )
        source = MassiveDataSource("k", PriceCache(), client=client)
        await source.start(["AAPL"])                   # first poll fails, doesn't raise
        assert source._backoff == 15.0
        await source._poll_once()
        assert source._backoff == 30.0
        await source.stop()

    def test_classify(self):
        assert classify_error(BadResponse('{"status":"NOT_AUTHORIZED"}')) == "forbidden"
        assert classify_error(ConnectionError()) == "transient"
```

### 15.3 Simulator statistics tests

```python
import numpy as np
import pytest

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator, seed_price_for


def test_deterministic_with_seed():
    a = GBMSimulator(["AAPL", "MSFT"], seed=42)
    b = GBMSimulator(["AAPL", "MSFT"], seed=42)
    assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]


def test_per_tick_volatility_calibrated():
    sim = GBMSimulator(["AAPL"], event_probability=0.0, seed=1)
    prices = [sim._prices["AAPL"]]
    for _ in range(20_000):
        sim.step()
        prices.append(sim._prices["AAPL"])
    log_returns = np.diff(np.log(prices))
    assert log_returns.std() == pytest.approx(0.22 * np.sqrt(GBMSimulator.DEFAULT_DT), rel=0.05)


def test_sector_correlation():
    sim = GBMSimulator(["AAPL", "MSFT", "JPM"], event_probability=0.0, seed=3)
    paths = {t: [sim._prices[t]] for t in sim.get_tickers()}
    for _ in range(20_000):
        sim.step()
        for t in paths:
            paths[t].append(sim._prices[t])
    r = {t: np.diff(np.log(p)) for t, p in paths.items()}
    assert np.corrcoef(r["AAPL"], r["MSFT"])[0, 1] == pytest.approx(0.6, abs=0.05)  # tech-tech
    assert np.corrcoef(r["AAPL"], r["JPM"])[0, 1] == pytest.approx(0.3, abs=0.05)   # cross


def test_cholesky_large_universe():
    sim = GBMSimulator(list(SEED_PRICES) + [f"ZZ{i}" for i in range(40)])
    assert sim._cholesky is not None


def test_unknown_seed_price_is_stable():
    assert seed_price_for("PYPL") == seed_price_for("PYPL")
    assert 50 <= seed_price_for("PYPL") < 300
    assert seed_price_for("AAPL") == SEED_PRICES["AAPL"]
```

### 15.4 SSE generator test (no server needed)

Test the generator directly with a stand-in request. That avoids the flakiness of streaming through `httpx`/`TestClient`:

```python
import json

from app.market.cache import PriceCache
from app.market.stream import generate_price_events


class FakeRequest:
    def __init__(self, polls_before_disconnect: int):
        self.client = None
        self._left = polls_before_disconnect

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def test_sse_frames():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = [f async for f in generate_price_events(cache, FakeRequest(3), interval=0.01, heartbeat=0.0)]
    assert frames[0] == "retry: 1000\n\n"
    data = json.loads(frames[1].removeprefix("data: "))
    assert data["AAPL"]["price"] == 190.0
    assert frames[2] == ": keep-alive\n\n"


async def test_sse_sends_empty_snapshot_after_last_removal():
    cache = PriceCache()
    cache.update("AAPL", 190.0)
    frames = []
    async for f in generate_price_events(cache, FakeRequest(5), interval=0.01, heartbeat=99):
        frames.append(f)
        if len(frames) == 2:
            cache.remove("AAPL")
    assert frames[-1] == "data: {}\n\n"
```

### 15.5 E2E (from `test/`, PLAN §12)

With no `MASSIVE_API_KEY` (simulator), Playwright checks that: 10 default tickers appear with prices within 2 s. Prices change within 3 s. Adding `PYPL` shows a price immediately. Removing a ticker removes its row. Buying a non-watchlist ticker then removing it from the watchlist keeps the position priced. After the browser goes offline and back online, the status dot returns to green.

---

## 16. Failure Modes

| Failure | Behaviour | User sees |
|---|---|---|
| Massive key invalid / plan lacks snapshots | ERROR log once with remedy, retry every ~135 s | Prices absent (`price: null`), trades on them → 400 with message. Positions valued at cost with `price_stale`. |
| Massive 429 / network / 5xx | WARNING, exponential backoff up to +120 s, auto-recovery | Prices freeze, then resume |
| Unknown symbol under Massive | Tracked, but no data; WARNING once per symbol | Watchlist row with `—`; trade → 400 |
| Unknown symbol under simulator | Deterministic seed price in $50–300 | Works like any ticker |
| Invalid symbol (`"12$"`) | `normalize_ticker` → `ValueError` | HTTP 400 `Invalid ticker symbol` |
| Simulator step raises (bug) | `logger.exception`, loop continues next tick | One missed tick |
| Correlation matrix not PD (bad constants) | WARNING, independent moves | Prices still move |
| Empty watchlist and no positions | Sources idle (Massive makes no requests) | `data: {}` + heartbeats |
| SSE client disconnects | Generator exits on `is_disconnected()` or cancellation | — |
| Server restart | Simulator restarts from seed prices (known) / CRC prices (unknown); `session_open` resets | Daily change resets to 0% (simulator) |

---

## 17. Implementation Checklist

Work in this order. Each step leaves the suite green.

1. **`tickers.py`**: add the module and its tests. Export `normalize_ticker` from `__init__.py`.
2. **`models.py` / `cache.py`**: add `session_open` + `day_change*`, `snapshot()`, versioned `remove()`, `timestamp is None`, locked `version`. Update `test_models.py` / `test_cache.py`. Every `PriceUpdate(...)` constructed in tests now needs `session_open`.
3. **`interface.py`**: update the docstring contract (no signature changes).
4. **`simulator.py`**: `seed_price_for`, `seed`-driven `Generator`, `add_tickers` batch, Cholesky fallback, eager `GBMSimulator`, double-start guard, normalization, `session_open` seeding. Add the statistics tests.
5. **`massive_client.py`**: `parse_snapshot`, `classify_error`, `client=` injection, `retries=0`, wake/backoff loop, in-flight removal guard. **Rewrite `test_massive.py` to use `TickerSnapshot.from_dict`**. Drop the `MagicMock` snapshots and the patches of `RESTClient`.
6. **`factory.py`**: `env` parameter, `MASSIVE_POLL_INTERVAL`. Add it to `.env.example`.
7. **`stream.py`**: router built inside the factory, `snapshot()`, empty frames, heartbeat, public `generate_price_events`. Add the generator tests.
8. **App layer** (when built): `create_app()` lifespan, `deps.py`, `market_sync.py`, and call `sync_tracked_tickers()` from the watchlist, trade and chat flows.
9. Run `uv run --extra dev pytest --cov=app` and `uv run --extra dev ruff check app/ tests/`. Update `MARKET_DATA_SUMMARY.md` and `backend/CLAUDE.md` (new `PriceUpdate` fields, `normalize_ticker`, `MASSIVE_POLL_INTERVAL`).
