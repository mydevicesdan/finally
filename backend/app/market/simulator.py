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
