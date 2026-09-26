# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Import only from the package:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource,
    create_market_data_source, create_stream_router,
    normalize_ticker, sync_tracked_tickers,
)
```

Full design: `planning/MARKET_DATA_DESIGN.md`.

### Core Types

- **`PriceUpdate`**: an immutable dataclass with the fields `ticker`, `price`, `previous_price`, `timestamp` (Unix seconds) and `session_open`. Its properties are:
  - `change`, `change_percent`, `direction` ("up"/"down"/"flat"): the move since the **previous update**. The frontend uses these for the flash.
  - `day_change`, `day_change_percent`: the move since `session_open`. Use these for the watchlist's "daily change %" and for the LLM context. `session_open` is the previous close under Massive, and the price when the ticker entered the simulation under the simulator.
  - `to_dict()` serializes all of the above for JSON/SSE.

- **`PriceCache`**: a thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, session_open=None) -> PriceUpdate`
  - `get(ticker) -> PriceUpdate | None`, `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]` (a copy)
  - `snapshot() -> (version, dict)`, read atomically
  - `remove(ticker)`. Only data sources call this; everything else goes through `source.remove_ticker()`.
  - `version`: a monotonic counter bumped by every update and every removal.
  - An empty cache is falsy (it defines `__len__`), so write `cache if cache is not None else ...`, never `cache or ...`.

- **`MarketDataSource`**: the abstract interface, implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)`, then `add_ticker()` / `remove_ticker()`, then `stop()`.
  - Tickers are normalized: `" aapl "` becomes `"AAPL"`, and invalid symbols raise `ValueError`.
  - `add_ticker()` works before `start()`. Calling `start()` a second time raises `RuntimeError`.
  - The simulator prices an added ticker immediately. Massive fetches it on the next allowed poll slot (at most 12 s later on the free tier).

- **`create_market_data_source(cache, env=None)`**: the factory. It returns `MassiveDataSource` if `MASSIVE_API_KEY` is set, otherwise `SimulatorDataSource`. `MASSIVE_POLL_INTERVAL` (seconds, default 15) sets how often Massive is polled.

- **`normalize_ticker(raw) -> str`**: use it in every route or LLM action that accepts a ticker, and map `ValueError` to HTTP 400.

### Which tickers to track (important for the portfolio/watchlist/chat code)

The source must track **watchlist ∪ tickers with an open position**. Otherwise a held ticker that is removed from the watchlist loses its price, and valuation and P&L break. After every watchlist change, trade, and LLM action batch, call:

```python
await sync_tracked_tickers(source, set(watchlist_tickers) | set(position_tickers))
```

To trade a ticker that has no cached price, call `await source.add_ticker(t)` first. Under the simulator that prices it instantly. If `cache.get_price(t)` is still `None` (Massive hasn't polled it yet, or the symbol is unknown), return HTTP 400.

### SSE Streaming

```python
from app.market import create_stream_router

router = create_stream_router(price_cache)  # a new APIRouter per call
# Endpoint: GET /api/stream/prices (text/event-stream)
```

Each `data:` frame is the **complete** map `{ticker: PriceUpdate.to_dict()}`, sent at most every 500 ms and only when the cache changed. `data: {}` means no tickers are tracked. A `: keep-alive` comment is sent after 15 s of inactivity. Clients should replace their map on each frame rather than merge into it.

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`. Unknown tickers get a deterministic seed price between $50 and $300, which stays the same across restarts.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
uv run --extra dev ruff format app/ tests/ # Format
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
