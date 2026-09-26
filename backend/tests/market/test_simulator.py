"""Tests for GBMSimulator."""

import numpy as np
import pytest

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator, seed_price_for


class TestGBMSimulator:
    """Unit tests for the GBM price simulator."""

    def test_step_returns_all_tickers(self):
        """Test that step() returns prices for all tickers."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        result = sim.step()
        assert set(result.keys()) == {"AAPL", "GOOGL"}

    def test_prices_are_positive(self):
        """GBM prices can never go negative (exp() is always positive)."""
        sim = GBMSimulator(tickers=["AAPL"])
        for _ in range(10_000):
            prices = sim.step()
            assert prices["AAPL"] > 0

    def test_initial_prices_match_seeds(self):
        """Test that initial prices match seed prices."""
        sim = GBMSimulator(tickers=["AAPL"])
        # Before any step, price should be the seed price
        assert sim.get_price("AAPL") == SEED_PRICES["AAPL"]

    def test_add_ticker(self):
        """Test adding a ticker dynamically."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("TSLA")
        result = sim.step()
        assert "TSLA" in result

    def test_remove_ticker(self):
        """Test removing a ticker."""
        sim = GBMSimulator(tickers=["AAPL", "GOOGL"])
        sim.remove_ticker("GOOGL")
        result = sim.step()
        assert "GOOGL" not in result
        assert "AAPL" in result

    def test_add_duplicate_is_noop(self):
        """Test that adding a duplicate ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.add_ticker("AAPL")
        assert len(sim._tickers) == 1

    def test_remove_nonexistent_is_noop(self):
        """Test that removing a non-existent ticker is a no-op."""
        sim = GBMSimulator(tickers=["AAPL"])
        sim.remove_ticker("NOPE")  # Should not raise

    def test_unknown_ticker_gets_seed_price_in_range(self):
        """Unknown tickers start between $50 and $300."""
        sim = GBMSimulator(tickers=["ZZZZ"])
        price = sim.get_price("ZZZZ")
        assert price is not None
        assert 50.0 <= price < 300.0

    def test_empty_step(self):
        """Test stepping with no tickers."""
        sim = GBMSimulator(tickers=[])
        result = sim.step()
        assert result == {}

    def test_prices_change_over_time(self):
        """After many steps, prices should have drifted from their seeds."""
        sim = GBMSimulator(tickers=["AAPL"])
        initial_price = sim.get_price("AAPL")

        for _ in range(1000):
            sim.step()

        final_price = sim.get_price("AAPL")
        # Price should have changed (extremely unlikely to be exactly the seed)
        assert final_price != initial_price

    def test_cholesky_rebuilds_on_add(self):
        """Test that Cholesky matrix is rebuilt when tickers are added."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None  # Only 1 ticker, no correlation matrix
        sim.add_ticker("GOOGL")
        assert sim._cholesky is not None  # Now 2 tickers, matrix exists

    def test_cholesky_none_with_one_ticker(self):
        """Test that Cholesky is None with only one ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim._cholesky is None

    def test_get_price_returns_none_for_unknown(self):
        """Test that get_price returns None for unknown ticker."""
        sim = GBMSimulator(tickers=["AAPL"])
        assert sim.get_price("UNKNOWN") is None

    def test_pairwise_correlation_tech_stocks(self):
        """Test that tech stocks have high correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "GOOGL")
        assert corr == 0.6

    def test_pairwise_correlation_finance_stocks(self):
        """Test that finance stocks have moderate correlation."""
        corr = GBMSimulator._pairwise_correlation("JPM", "V")
        assert corr == 0.5

    def test_pairwise_correlation_tsla(self):
        """Test that TSLA has lower correlation with everything."""
        corr = GBMSimulator._pairwise_correlation("TSLA", "AAPL")
        assert corr == 0.3
        corr = GBMSimulator._pairwise_correlation("TSLA", "JPM")
        assert corr == 0.3

    def test_pairwise_correlation_cross_sector(self):
        """Test cross-sector correlation."""
        corr = GBMSimulator._pairwise_correlation("AAPL", "JPM")
        assert corr == 0.3

    def test_default_dt_is_reasonable(self):
        """Test that default dt is a reasonable small value."""
        assert 0 < GBMSimulator.DEFAULT_DT < 0.0001

    def test_prices_rounded_to_two_decimals(self):
        """Test that prices are rounded to 2 decimal places."""
        sim = GBMSimulator(tickers=["AAPL"])
        result = sim.step()
        price_str = str(result["AAPL"])
        # Check that we have at most 2 decimal places
        if "." in price_str:
            decimal_part = price_str.split(".")[1]
            assert len(decimal_part) <= 2


class TestGBMSimulatorDeterminismAndSeeding:
    """Seeded RNG and deterministic starting prices."""

    def test_same_seed_same_path(self):
        a = GBMSimulator(["AAPL", "MSFT"], seed=42)
        b = GBMSimulator(["AAPL", "MSFT"], seed=42)
        assert [a.step() for _ in range(50)] == [b.step() for _ in range(50)]

    def test_different_seed_different_path(self):
        a = GBMSimulator(["AAPL"], seed=1)
        b = GBMSimulator(["AAPL"], seed=2)
        assert [a.step() for _ in range(50)] != [b.step() for _ in range(50)]

    def test_unknown_seed_price_is_stable_across_instances(self):
        """A restart must not re-price a held position."""
        assert GBMSimulator(["PYPL"]).get_price("PYPL") == GBMSimulator(["PYPL"]).get_price("PYPL")

    def test_seed_price_for(self):
        assert seed_price_for("AAPL") == SEED_PRICES["AAPL"]
        assert seed_price_for("PYPL") == seed_price_for("PYPL")
        assert 50.0 <= seed_price_for("PYPL") < 300.0
        assert seed_price_for("PYPL") != seed_price_for("SHOP")

    def test_add_tickers_batch_single_rebuild(self, monkeypatch):
        sim = GBMSimulator()
        calls = []
        original = sim._rebuild_cholesky

        def counting_rebuild():
            calls.append(1)
            original()

        monkeypatch.setattr(sim, "_rebuild_cholesky", counting_rebuild)
        sim.add_tickers(["AAPL", "MSFT", "JPM", "AAPL"])
        assert len(calls) == 1
        assert sim.get_tickers() == ["AAPL", "MSFT", "JPM"]

    def test_add_existing_only_does_not_rebuild(self, monkeypatch):
        sim = GBMSimulator(["AAPL"])
        monkeypatch.setattr(sim, "_rebuild_cholesky", lambda: pytest.fail("rebuilt"))
        sim.add_tickers(["AAPL"])


class TestGBMSimulatorStatistics:
    """The simulated paths have the advertised statistical properties."""

    def test_prices_stay_positive_with_frequent_shocks(self):
        sim = GBMSimulator(["TSLA"], event_probability=0.05, seed=7)
        for _ in range(10_000):
            assert sim.step()["TSLA"] > 0

    def test_per_tick_volatility_calibrated(self):
        sim = GBMSimulator(["AAPL"], event_probability=0.0, seed=1)
        prices = [sim._prices["AAPL"]]
        for _ in range(20_000):
            sim.step()
            prices.append(sim._prices["AAPL"])
        log_returns = np.diff(np.log(prices))
        expected = 0.22 * np.sqrt(GBMSimulator.DEFAULT_DT)
        assert log_returns.std() == pytest.approx(expected, rel=0.05)

    def test_sector_correlation(self):
        sim = GBMSimulator(["AAPL", "MSFT", "JPM", "V"], event_probability=0.0, seed=3)
        paths = {t: [sim._prices[t]] for t in sim.get_tickers()}
        for _ in range(20_000):
            sim.step()
            for t in paths:
                paths[t].append(sim._prices[t])
        r = {t: np.diff(np.log(p)) for t, p in paths.items()}
        assert np.corrcoef(r["AAPL"], r["MSFT"])[0, 1] == pytest.approx(0.6, abs=0.05)
        assert np.corrcoef(r["JPM"], r["V"])[0, 1] == pytest.approx(0.5, abs=0.05)
        assert np.corrcoef(r["AAPL"], r["JPM"])[0, 1] == pytest.approx(0.3, abs=0.05)

    def test_shock_moves_price_two_to_five_percent(self):
        sim = GBMSimulator(["AAPL"], event_probability=1.0, seed=5)
        for _ in range(100):
            before = sim._prices["AAPL"]
            sim.step()
            move = abs(sim._prices["AAPL"] / before - 1)
            assert 0.019 < move < 0.051  # 2-5% jump plus a tiny diffusion term

    def test_cholesky_for_full_default_set(self):
        sim = GBMSimulator(list(SEED_PRICES))
        assert sim._cholesky is not None
        assert sim._cholesky.shape == (10, 10)

    def test_cholesky_large_universe(self):
        sim = GBMSimulator(list(SEED_PRICES) + [f"ZZ{i}" for i in range(40)])
        assert sim._cholesky is not None

    def test_cholesky_failure_falls_back_to_independent(self, monkeypatch):
        def boom(_):
            raise np.linalg.LinAlgError("not positive definite")

        monkeypatch.setattr(np.linalg, "cholesky", boom)
        sim = GBMSimulator(["AAPL", "MSFT"])
        assert sim._cholesky is None
        assert set(sim.step()) == {"AAPL", "MSFT"}
