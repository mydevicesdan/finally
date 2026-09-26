"""Tests for sync_tracked_tickers."""

import pytest

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource
from app.market.sync import sync_tracked_tickers


@pytest.fixture
async def source():
    src = SimulatorDataSource(price_cache=PriceCache(), update_interval=10)
    await src.start(["AAPL", "MSFT"])
    yield src
    await src.stop()


async def test_adds_and_removes(source):
    await sync_tracked_tickers(source, {"MSFT", "TSLA"})
    assert sorted(source.get_tickers()) == ["MSFT", "TSLA"]
    assert "AAPL" not in source._cache
    assert source._cache.get_price("TSLA") is not None


async def test_held_position_stays_tracked(source):
    """Watchlist ∪ positions: removing from the watchlist keeps a held ticker priced."""
    watchlist = {"MSFT"}
    positions = {"AAPL"}
    await sync_tracked_tickers(source, watchlist | positions)
    assert sorted(source.get_tickers()) == ["AAPL", "MSFT"]
    assert source._cache.get_price("AAPL") is not None


async def test_normalizes_input(source):
    await sync_tracked_tickers(source, ["aapl", " msft "])
    assert sorted(source.get_tickers()) == ["AAPL", "MSFT"]


async def test_noop_when_in_sync(source):
    v = source._cache.version
    await sync_tracked_tickers(source, {"AAPL", "MSFT"})
    assert source._cache.version == v


async def test_empty_set_removes_everything(source):
    await sync_tracked_tickers(source, set())
    assert source.get_tickers() == []
    assert len(source._cache) == 0
