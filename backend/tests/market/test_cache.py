"""Tests for PriceCache."""

import threading

from app.market.cache import PriceCache


class TestPriceCache:
    """Unit tests for the PriceCache."""

    def test_update_and_get(self):
        """Test updating and getting a price."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        """Test that the first update has flat direction."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        """Test price update with upward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        """Test price update with downward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove(self):
        """Test removing a ticker from cache."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent(self):
        """Test removing a ticker that doesn't exist."""
        cache = PriceCache()
        cache.remove("AAPL")  # Should not raise

    def test_get_all(self):
        """Test getting all prices."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments(self):
        """Test that version counter increments."""
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        """Test the convenience get_price method."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_len(self):
        """Test __len__ method."""
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.00)
        assert len(cache) == 1
        cache.update("GOOGL", 175.00)
        assert len(cache) == 2

    def test_contains(self):
        """Test __contains__ method."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        assert "AAPL" in cache
        assert "GOOGL" not in cache

    def test_custom_timestamp(self):
        """Test updating with a custom timestamp."""
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts

    def test_price_rounding(self):
        """Test that prices are rounded to 2 decimal places."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12

    def test_zero_timestamp_respected(self):
        """A timestamp of 0.0 is a real value, not 'missing'."""
        cache = PriceCache()
        assert cache.update("AAPL", 190.0, timestamp=0.0).timestamp == 0.0

    def test_remove_bumps_version(self):
        """Removal is a change the SSE stream must see."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        v = cache.version
        cache.remove("AAPL")
        assert cache.version == v + 1

    def test_remove_nonexistent_keeps_version(self):
        cache = PriceCache()
        v = cache.version
        cache.remove("AAPL")
        assert cache.version == v

    def test_session_open_defaults_to_first_price(self):
        cache = PriceCache()
        update = cache.update("AAPL", 190.00)
        assert update.session_open == 190.00
        assert update.day_change == 0.0

    def test_session_open_explicit_then_kept(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00, session_open=180.00)
        update = cache.update("AAPL", 191.00)
        assert update.session_open == 180.00
        assert update.previous_price == 190.00
        assert update.day_change == 11.00

    def test_session_open_can_be_replaced(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00, session_open=180.00)
        assert cache.update("AAPL", 190.00, session_open=185.00).session_open == 185.00

    def test_snapshot_is_consistent(self):
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("MSFT", 420.00)
        version, prices = cache.snapshot()
        assert version == cache.version == 2
        assert set(prices) == {"AAPL", "MSFT"}
        prices.clear()  # a copy: mutating it must not affect the cache
        assert len(cache) == 2

    def test_concurrent_writers(self):
        """Every update from every thread is counted exactly once."""
        cache = PriceCache()
        tickers = [f"T{i}" for i in range(8)]

        def writer(ticker: str) -> None:
            for n in range(1000):
                cache.update(ticker, 100.0 + n % 7)

        threads = [threading.Thread(target=writer, args=(t,)) for t in tickers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert cache.version == 8 * 1000
        assert set(cache.get_all()) == set(tickers)
