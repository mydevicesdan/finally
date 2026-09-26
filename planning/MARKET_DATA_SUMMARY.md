# Market Data Backend — Summary

**Status:** Complete and tested. Every finding in `MARKET_DATA_REVIEW.md` is fixed, and the backend is ready for the portfolio, watchlist, chat and frontend layers.

Detailed design, with the module code: `MARKET_DATA_DESIGN.md`. Review and resolution log: `MARKET_DATA_REVIEW.md`. Usage guide for backend agents: `backend/CLAUDE.md`.

## What Was Built

A market data subsystem in `backend/app/market/` (11 modules, about 940 lines). It produces live simulated prices, or real prices from the Massive API, behind one interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  correlated GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Massive/Polygon REST snapshot poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, versioned)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation / snapshots
        ├──→ Trade execution
        └──→ LLM context
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate`, a frozen dataclass: ticker, price, previous_price, timestamp, session_open. Properties give tick change/direction and day change/% |
| `cache.py` | `PriceCache`: thread-safe store. Version bumps on update and remove. Atomic `snapshot()` |
| `interface.py` | `MarketDataSource`: abstract base class with `start/stop/add_ticker/remove_ticker/get_tickers` and a documented contract |
| `tickers.py` | `normalize_ticker()`: trim, upper-case, validate. Used by both sources and API routes |
| `seed_prices.py` | Seed prices, per-ticker GBM drift/volatility, correlation groups |
| `simulator.py` | `GBMSimulator` (seedable, Cholesky-correlated, 2–5% shock events) + `SimulatorDataSource` |
| `massive_client.py` | `parse_snapshot()`, `classify_error()`, `MassiveDataSource` (rate-limit spacing, backoff, early poll on add) |
| `factory.py` | `create_market_data_source()`: reads `MASSIVE_API_KEY`, `MASSIVE_POLL_INTERVAL` |
| `stream.py` | `create_stream_router()` + `generate_price_events()`: full-snapshot SSE with keep-alive |
| `sync.py` | `sync_tracked_tickers()`: keeps the source tracking watchlist ∪ open positions |

### Key Design Decisions

- **Strategy pattern plus a single cache.** Producers write to the cache and consumers read from it, so no consumer depends on which source is active.
- **GBM with correlated moves.** Tech-to-tech correlation is 0.6, finance-to-finance 0.5, and cross-sector or TSLA 0.3. Per-tick volatility is calibrated to real intraday ranges (checked statistically in tests).
- **Two change references.** Tick-to-tick change drives the price flash. Change since `session_open` drives the daily change % (previous close under Massive, simulation start under the simulator).
- **Deterministic starting prices.** Unknown tickers get a CRC-derived seed price, so held positions don't re-price randomly on restart.
- **Full-snapshot SSE.** Every frame is the whole ticker map, so clients replace rather than merge, and removals and empty watchlists stream correctly.
- **Massive safety.** The SDK's own retries are off. The poll loop handles retries: at most 5 requests/min by default, exponential backoff, and auth/plan errors logged once with a remedy.

## Test Suite

**174 tests, all passing, 100% line coverage.** `ruff check` and `ruff format --check` are clean. The suite runs in about 2–4 s and was stable across repeated runs.

| Module | Tests | Focus |
|--------|------:|-------|
| test_massive.py | 41 | Real SDK models via `TickerSnapshot.from_dict`, URL built by the real `RESTClient`, fallbacks, backoff, removal race, early poll |
| test_simulator.py | 32 | GBM behaviour, determinism, σ calibration, sector correlation, shocks, Cholesky fallback |
| test_cache.py | 21 | Versioning (incl. removal), session_open, snapshot, concurrent writers |
| test_tickers.py | 21 | Normalization and validation |
| test_simulator_source.py | 17 | Lifecycle, normalization, add-before-start, exception resilience |
| test_models.py | 14 | Tick and day change, serialization |
| test_factory.py | 13 | Env selection, poll interval parsing |
| test_stream.py | 10 | SSE frames, empty snapshots, removals, heartbeat, headers, router independence |
| test_sync.py | 5 | Watchlist ∪ positions reconciliation |

It was also verified end to end under uvicorn with `curl`: frames arrive every 500 ms, a ticker added at runtime is priced immediately, and removing every ticker streams `data: {}`.

**Not verified:** a live call to the Massive API. No API key was available, and outbound access to `api.massive.com` was blocked. Response parsing and URL construction are tested against the real SDK code instead.

## Demo

```bash
cd backend
uv run market_data_demo.py
```

Shows a live Rich terminal dashboard with all 10 tickers, sparklines, direction arrows and an event log. It runs for 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source, sync_tracked_tickers

# Startup (inside the FastAPI lifespan)
cache = PriceCache()
source = create_market_data_source(cache)          # reads MASSIVE_API_KEY
await source.start(watchlist_tickers | position_tickers)

# Read prices
update = cache.get("AAPL")                          # PriceUpdate or None
price = cache.get_price("AAPL")                     # float or None
update.day_change_percent                           # watchlist "daily change %"

# After any watchlist change, trade, or LLM action batch
await sync_tracked_tickers(source, watchlist_tickers | position_tickers)

# Shutdown
await source.stop()
```
