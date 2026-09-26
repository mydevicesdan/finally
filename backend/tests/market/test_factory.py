"""Tests for market data source factory."""

import os
from unittest.mock import patch

import pytest

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestFactory:
    """Tests for create_market_data_source."""

    @pytest.mark.parametrize("env", [{}, {"MASSIVE_API_KEY": ""}, {"MASSIVE_API_KEY": "   "}])
    def test_creates_simulator_without_api_key(self, env):
        assert isinstance(create_market_data_source(PriceCache(), env=env), SimulatorDataSource)

    def test_creates_massive_when_api_key_set(self):
        source = create_market_data_source(PriceCache(), env={"MASSIVE_API_KEY": "test-key"})
        assert isinstance(source, MassiveDataSource)

    def test_massive_receives_stripped_api_key(self):
        source = create_market_data_source(PriceCache(), env={"MASSIVE_API_KEY": " test-key-123 "})
        assert source._client.API_KEY == "test-key-123"

    def test_sources_receive_cache(self):
        cache = PriceCache()
        assert create_market_data_source(cache, env={})._cache is cache
        assert create_market_data_source(cache, env={"MASSIVE_API_KEY": "k"})._cache is cache

    def test_default_poll_interval(self):
        source = create_market_data_source(PriceCache(), env={"MASSIVE_API_KEY": "k"})
        assert source._interval == 15.0
        assert source._min_spacing == 12.0

    def test_custom_poll_interval(self):
        env = {"MASSIVE_API_KEY": "k", "MASSIVE_POLL_INTERVAL": "3"}
        source = create_market_data_source(PriceCache(), env=env)
        assert source._interval == 3.0
        assert source._min_spacing == 3.0

    @pytest.mark.parametrize("raw", ["abc", "0", "-5", "  "])
    def test_invalid_poll_interval_falls_back(self, raw):
        env = {"MASSIVE_API_KEY": "k", "MASSIVE_POLL_INTERVAL": raw}
        assert create_market_data_source(PriceCache(), env=env)._interval == 15.0

    def test_reads_os_environ_by_default(self):
        with patch.dict(os.environ, {"MASSIVE_API_KEY": "from-env"}, clear=True):
            source = create_market_data_source(PriceCache())
        assert isinstance(source, MassiveDataSource)
        with patch.dict(os.environ, {}, clear=True):
            assert isinstance(create_market_data_source(PriceCache()), SimulatorDataSource)
