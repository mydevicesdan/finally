"""Integration tests for SimulatorDataSource."""

import asyncio

import pytest

from app.market.cache import PriceCache
from app.market.simulator import SimulatorDataSource, seed_price_for


async def wait_for(predicate, timeout: float = 2.0) -> None:
    """Poll `predicate` until true; generous timeout keeps CI from flaking."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


class TestSimulatorDataSource:
    async def test_start_populates_cache(self):
        """Seed prices are in the cache before the first tick."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.start(["AAPL", "GOOGL"])
        assert cache.get_price("AAPL") == seed_price_for("AAPL")
        assert cache.get("GOOGL").session_open == seed_price_for("GOOGL")
        await source.stop()

    async def test_prices_update_over_time(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])
        initial_version = cache.version
        await wait_for(lambda: cache.version >= initial_version + 3)
        await source.stop()

    async def test_session_open_kept_across_ticks(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01, seed=1)
        await source.start(["AAPL"])
        v = cache.version
        await wait_for(lambda: cache.version >= v + 5)
        assert cache.get("AAPL").session_open == seed_price_for("AAPL")
        await source.stop()

    async def test_stop_is_clean_and_idempotent(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])
        task = source._task
        await source.stop()
        await source.stop()
        assert task.done()
        v = cache.version
        await asyncio.sleep(0.05)
        assert cache.version == v  # no writes after stop()

    async def test_start_twice_raises(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=10)
        await source.start(["AAPL"])
        with pytest.raises(RuntimeError):
            await source.start(["MSFT"])
        await source.stop()

    async def test_start_normalizes_and_dedupes(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.start(["aapl", " AAPL", "msft"])
        assert source.get_tickers() == ["AAPL", "MSFT"]
        assert set(cache.get_all()) == {"AAPL", "MSFT"}
        await source.stop()

    async def test_add_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.start(["AAPL"])
        await source.add_ticker("TSLA")
        assert "TSLA" in source.get_tickers()
        assert cache.get_price("TSLA") == seed_price_for("TSLA")  # priced immediately
        await source.stop()

    async def test_add_ticker_normalizes(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.start(["AAPL"])
        await source.add_ticker(" aapl ")
        await source.add_ticker("pypl")
        assert source.get_tickers() == ["AAPL", "PYPL"]
        assert "pypl" not in cache
        await source.stop()

    async def test_add_existing_does_not_reset_price(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.start(["AAPL"])
        cache.update("AAPL", 123.45)
        v = cache.version
        await source.add_ticker("AAPL")
        assert cache.version == v
        await source.stop()

    async def test_add_invalid_ticker_raises(self):
        source = SimulatorDataSource(price_cache=PriceCache())
        with pytest.raises(ValueError):
            await source.add_ticker("BAD TICKER")

    async def test_add_ticker_before_start(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=10)
        await source.add_ticker("PYPL")
        await source.start(["AAPL"])
        assert source.get_tickers() == ["PYPL", "AAPL"]
        assert cache.get_price("PYPL") == seed_price_for("PYPL")
        await source.stop()

    async def test_remove_ticker(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL", "TSLA"])
        await source.remove_ticker("tsla")
        assert source.get_tickers() == ["AAPL"]
        assert cache.get("TSLA") is None
        v = cache.version
        await wait_for(lambda: cache.version > v + 2)
        assert "TSLA" not in cache  # never written back by later ticks
        await source.stop()

    async def test_remove_unknown_is_noop(self):
        source = SimulatorDataSource(price_cache=PriceCache(), update_interval=10)
        await source.start(["AAPL"])
        await source.remove_ticker("MSFT")
        assert source.get_tickers() == ["AAPL"]
        await source.stop()

    async def test_empty_start(self):
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start([])
        await asyncio.sleep(0.03)
        assert len(cache) == 0
        assert source.get_tickers() == []
        await source.stop()

    async def test_loop_survives_step_exception(self, monkeypatch):
        """An exception inside step() is logged and the loop keeps running."""
        cache = PriceCache()
        source = SimulatorDataSource(price_cache=cache, update_interval=0.01)
        await source.start(["AAPL"])

        calls = {"n": 0}
        real_step = source._sim.step

        def flaky_step():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return real_step()

        monkeypatch.setattr(source._sim, "step", flaky_step)
        v = cache.version
        await wait_for(lambda: calls["n"] >= 3 and cache.version > v)
        assert not source._task.done()
        await source.stop()

    async def test_event_probability_is_passed_through(self):
        cache = PriceCache()
        source = SimulatorDataSource(
            price_cache=cache, update_interval=0.01, event_probability=1.0, seed=2
        )
        await source.start(["AAPL"])
        v = cache.version
        await wait_for(lambda: cache.version > v)
        update = cache.get("AAPL")
        # Every tick is a 2-5% shock, so the first tick moves at least ~2%.
        assert abs(update.price / update.previous_price - 1) > 0.019
        await source.stop()

    async def test_seed_makes_source_deterministic(self):
        async def first_ticks(seed: int) -> list[float]:
            cache = PriceCache()
            source = SimulatorDataSource(price_cache=cache, update_interval=10, seed=seed)
            await source.start(["AAPL"])
            await source.stop()
            return [source._sim.step()["AAPL"] for _ in range(20)]

        assert await first_ticks(9) == await first_ticks(9)
