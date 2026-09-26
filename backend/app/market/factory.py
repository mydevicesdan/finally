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
