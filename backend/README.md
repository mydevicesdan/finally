# FinAlly Backend

FastAPI backend for the FinAlly AI Trading Workstation.

## Structure

- `app/` - Application code
  - `market/` - Market data subsystem
    - `models.py` - PriceUpdate dataclass
    - `cache.py` - Thread-safe price cache
    - `interface.py` - MarketDataSource abstract interface
    - `simulator.py` - GBM-based market simulator
    - `massive_client.py` - Massive/Polygon.io API client
    - `factory.py` - Data source factory
    - `stream.py` - SSE streaming endpoint
    - `seed_prices.py` - Default ticker prices and parameters

- `tests/` - Unit and integration tests
  - `market/` - Market data tests

- `market_data_demo.py` - Rich terminal dashboard demo for the market data simulator

## Demo

A live terminal dashboard for the market data simulator, built with [Rich](https://github.com/Textualize/rich):

```bash
uv run market_data_demo.py
```

Shows all 10 default tickers streaming simulated prices with sparklines, direction arrows, green/red coloring, and a "recent events" log for moves greater than 1%. Runs for 60 seconds (or press Ctrl+C to exit early) and prints a session summary comparing final prices to seed prices.

## Running Tests

```bash
# Install dependencies
uv sync --dev

# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html

# Run specific test file
uv run pytest tests/market/test_simulator.py

# Run with verbose output
uv run pytest -v
```

## Environment Variables

- `MASSIVE_API_KEY` - Optional. If set, use real market data from Massive API. If not set, use the built-in simulator.

## Development

```bash
# Install dependencies
uv sync --dev

# Run linter
uv run ruff check .

# Format code
uv run ruff format .
```
