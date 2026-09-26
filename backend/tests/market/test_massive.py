"""Tests for the Massive data source.

Snapshots are built with the real SDK model (`TickerSnapshot.from_dict`) from
wire-shaped JSON, so field names and units are checked against massive itself.
The HTTP client is replaced by a MagicMock injected via `client=`.
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest
from massive import RESTClient
from massive.exceptions import AuthError, BadResponse
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import (
    MAX_BACKOFF,
    MassiveDataSource,
    classify_error,
    parse_snapshot,
)

# One element of the snapshot response's "tickers" array, exactly as on the wire.
AAPL_JSON = {
    "ticker": "AAPL",
    "todaysChange": 1.25,
    "todaysChangePerc": 0.66,
    "updated": 1727366399123456789,
    "day": {"o": 189.1, "h": 191.2, "l": 188.7, "c": 190.4, "v": 48211234, "vw": 190.02},
    "min": {
        "av": 48211234,
        "o": 190.3,
        "h": 190.5,
        "l": 190.2,
        "c": 190.4,
        "v": 12000,
        "vw": 190.35,
        "t": 1727366340000,
        "n": 120,
    },
    "prevDay": {"o": 187.5, "h": 189.6, "l": 187.1, "c": 189.17, "v": 51000000, "vw": 188.9},
    "lastTrade": {
        "p": 190.42,
        "s": 100,
        "x": 4,
        "t": 1727366399123456789,
        "c": [14, 41],
        "i": "52983525029461",
    },
    "lastQuote": {"P": 190.43, "S": 2, "p": 190.41, "s": 3, "t": 1727366399223456789},
}


def snap(data: dict) -> TickerSnapshot:
    return TickerSnapshot.from_dict(data)


def aapl(**overrides) -> TickerSnapshot:
    return snap({**AAPL_JSON, **overrides})


def make_source(cache: PriceCache | None = None, **kwargs) -> tuple[MassiveDataSource, MagicMock]:
    client = MagicMock()
    client.get_snapshot_all.return_value = []
    source = MassiveDataSource(
        "test-key", PriceCache() if cache is None else cache, client=client, **kwargs
    )
    return source, client


class TestParseSnapshot:
    def test_parses_real_wire_shape(self):
        q = parse_snapshot(snap(AAPL_JSON))
        assert q is not None
        assert q.ticker == "AAPL"
        assert q.price == 190.42
        assert q.timestamp == pytest.approx(1727366399.123456789)  # ns -> s
        assert q.session_open == 189.17

    def test_minute_bar_fallback(self):
        data = {k: v for k, v in AAPL_JSON.items() if k != "lastTrade"}
        q = parse_snapshot(snap(data))
        assert q.price == 190.4
        assert q.timestamp == pytest.approx(1727366340.0)  # ms -> s

    def test_day_close_fallback(self):
        data = {"ticker": "AAPL", "day": {"c": 190.4}, "updated": 1727366399000000000}
        q = parse_snapshot(snap(data))
        assert q.price == 190.4
        assert q.timestamp == pytest.approx(1727366399.0)
        assert q.session_open is None

    def test_prev_close_fallback(self):
        data = {"ticker": "AAPL", "prevDay": {"c": 189.17}, "updated": 1727366399000000000}
        q = parse_snapshot(snap(data))
        assert q.price == 189.17
        assert q.session_open == 189.17

    def test_zero_price_trade_ignored(self):
        q = parse_snapshot(aapl(lastTrade={"p": 0, "t": 1}))
        assert q.price == 190.4  # falls through to the minute bar

    def test_no_timestamp_uses_now(self):
        before = time.time()
        q = parse_snapshot(snap({"ticker": "AAPL", "day": {"c": 1.0}}))
        assert before <= q.timestamp <= time.time()

    def test_no_price_returns_none(self):
        assert parse_snapshot(snap({"ticker": "ZZZZ"})) is None

    def test_no_ticker_returns_none(self):
        assert parse_snapshot(snap({"lastTrade": {"p": 1.0}})) is None


class TestClassifyError:
    def test_auth_error(self):
        assert classify_error(AuthError("no key")) == "auth"

    def test_unknown_key(self):
        assert classify_error(BadResponse('{"status":"ERROR","error":"Unknown API Key"}')) == "auth"

    def test_not_authorized(self):
        body = '{"status":"NOT_AUTHORIZED","message":"You are not entitled to this data."}'
        assert classify_error(BadResponse(body)) == "forbidden"

    def test_rate_limit(self):
        body = '{"status":"ERROR","error":"You\'ve exceeded the maximum requests per minute"}'
        assert classify_error(BadResponse(body)) == "rate_limit"

    def test_transient(self):
        assert classify_error(ConnectionError("reset")) == "transient"
        assert classify_error(BadResponse("internal error")) == "transient"


class TestPolling:
    async def test_start_primes_cache(self):
        cache = PriceCache()
        source, client = make_source(cache)
        client.get_snapshot_all.return_value = [snap(AAPL_JSON)]
        await source.start(["AAPL"])

        update = cache.get("AAPL")
        assert update.price == 190.42
        assert update.session_open == 189.17
        assert round(update.day_change_percent, 2) == 0.66  # matches todaysChangePerc
        assert update.timestamp == pytest.approx(1727366399.123456789)
        await source.stop()

    async def test_requests_all_tickers_in_one_call(self):
        source, client = make_source()
        await source.start(["AAPL", "MSFT", "GOOGL"])
        client.get_snapshot_all.assert_called_once()
        assert client.get_snapshot_all.call_args.kwargs["tickers"] == ["AAPL", "MSFT", "GOOGL"]
        await source.stop()

    async def test_start_normalizes_and_dedupes(self):
        source, client = make_source()
        await source.start(["aapl", " AAPL ", "msft"])
        assert source.get_tickers() == ["AAPL", "MSFT"]
        await source.stop()

    async def test_start_twice_raises(self):
        source, _ = make_source()
        await source.start(["AAPL"])
        with pytest.raises(RuntimeError):
            await source.start(["AAPL"])
        await source.stop()

    async def test_unusable_snapshot_skipped(self):
        cache = PriceCache()
        source, client = make_source(cache)
        client.get_snapshot_all.return_value = [snap(AAPL_JSON), snap({"ticker": "BAD"})]
        await source.start(["AAPL", "BAD"])
        assert cache.get_price("AAPL") == 190.42
        assert cache.get("BAD") is None
        await source.stop()

    async def test_untracked_ticker_in_response_ignored(self):
        cache = PriceCache()
        source, client = make_source(cache)
        client.get_snapshot_all.return_value = [snap(AAPL_JSON), aapl(ticker="MSFT")]
        await source.start(["AAPL"])
        assert "MSFT" not in cache
        await source.stop()

    async def test_missing_ticker_warned_once(self, caplog):
        source, client = make_source()
        await source.start(["AAPL", "ZZZZ"])
        await source._poll_once()
        warnings = [r for r in caplog.records if "ZZZZ" in r.getMessage()]
        assert len(warnings) == 1
        await source.stop()

    async def test_empty_tickers_skips_request(self):
        source, client = make_source()
        await source.start([])
        client.get_snapshot_all.assert_not_called()
        await source.stop()

    async def test_removed_during_fetch_not_reinserted(self):
        cache = PriceCache()
        source, _ = make_source(cache)
        source._tickers = ["AAPL"]

        def slow_fetch(tickers):
            time.sleep(0.05)  # runs in the worker thread
            return [snap(AAPL_JSON)]

        source._fetch_snapshots = slow_fetch
        poll = asyncio.create_task(source._poll_once())
        await asyncio.sleep(0.01)
        await source.remove_ticker("AAPL")
        await poll
        assert "AAPL" not in cache

    async def test_fetch_receives_copy_of_tickers(self):
        source, client = make_source()
        await source.start(["AAPL"])
        passed = client.get_snapshot_all.call_args.kwargs["tickers"]
        assert passed is not source._tickers
        await source.stop()

    async def test_add_ticker_polls_early(self):
        cache = PriceCache()
        source, client = make_source(cache, poll_interval=60, min_poll_spacing=0)
        await source.start(["MSFT"])
        client.get_snapshot_all.return_value = [snap(AAPL_JSON)]
        await source.add_ticker("aapl")
        await asyncio.sleep(0.1)  # far less than the 60 s interval
        assert cache.get_price("AAPL") == 190.42
        await source.stop()

    async def test_early_poll_respects_min_spacing(self):
        source, client = make_source(poll_interval=60, min_poll_spacing=30)
        await source.start(["MSFT"])
        await source.add_ticker("AAPL")
        await asyncio.sleep(0.1)
        assert client.get_snapshot_all.call_count == 1  # still waiting for the 30 s slot
        await source.stop()

    async def test_loop_polls_on_interval(self):
        source, client = make_source(poll_interval=0.02, min_poll_spacing=0)
        await source.start(["AAPL"])
        await asyncio.sleep(0.15)
        assert client.get_snapshot_all.call_count >= 3
        await source.stop()


class TestFailures:
    async def test_error_does_not_raise(self):
        cache = PriceCache()
        source, client = make_source(cache)
        client.get_snapshot_all.side_effect = ConnectionError("network down")
        await source.start(["AAPL"])  # must not raise
        assert cache.get("AAPL") is None
        await source.stop()

    async def test_backoff_grows_and_caps(self):
        source, client = make_source(poll_interval=15)
        client.get_snapshot_all.side_effect = BadResponse("exceeded the maximum requests")
        source._tickers = ["AAPL"]
        backoffs = []
        for _ in range(6):
            await source._poll_once()
            backoffs.append(source._backoff)
        assert backoffs == [15, 30, 60, 120, 120, 120]

    async def test_auth_error_backs_off_to_max(self):
        source, client = make_source()
        client.get_snapshot_all.side_effect = BadResponse('{"status":"NOT_AUTHORIZED"}')
        source._tickers = ["AAPL"]
        await source._poll_once()
        assert source._backoff == MAX_BACKOFF

    async def test_success_resets_backoff(self):
        cache = PriceCache()
        source, client = make_source(cache)
        source._tickers = ["AAPL"]
        client.get_snapshot_all.side_effect = ConnectionError()
        await source._poll_once()
        assert source._backoff > 0
        client.get_snapshot_all.side_effect = None
        client.get_snapshot_all.return_value = [snap(AAPL_JSON)]
        await source._poll_once()
        assert source._backoff == 0
        assert source._failures == 0
        assert cache.get_price("AAPL") == 190.42


class TestTickerManagement:
    async def test_add_ticker_before_start(self):
        source, client = make_source()
        await source.add_ticker("aapl")
        await source.start(["MSFT"])
        assert source.get_tickers() == ["AAPL", "MSFT"]
        await source.stop()

    async def test_add_ticker_normalizes(self):
        source, _ = make_source()
        await source.add_ticker("  aapl  ")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_duplicate_is_noop(self):
        source, _ = make_source()
        await source.add_ticker("AAPL")
        await source.add_ticker("aapl")
        assert source.get_tickers() == ["AAPL"]

    async def test_add_invalid_raises(self):
        source, _ = make_source()
        with pytest.raises(ValueError):
            await source.add_ticker("NOT A TICKER")

    async def test_remove_ticker(self):
        cache = PriceCache()
        source, _ = make_source(cache)
        await source.add_ticker("AAPL")
        await source.add_ticker("GOOGL")
        cache.update("AAPL", 190.00)
        await source.remove_ticker("aapl")
        assert source.get_tickers() == ["GOOGL"]
        assert cache.get("AAPL") is None

    async def test_remove_unknown_is_noop(self):
        source, _ = make_source()
        await source.remove_ticker("AAPL")
        assert source.get_tickers() == []


class TestLifecycle:
    async def test_stop_is_idempotent(self):
        source, _ = make_source()
        await source.stop()
        await source.stop()

    async def test_stop_cancels_task(self):
        source, _ = make_source()
        await source.start(["AAPL"])
        task = source._task
        assert task is not None and not task.done()
        await source.stop()
        assert task.done()
        assert source._task is None

    async def test_request_url_built_by_real_sdk(self):
        """Drive the real RESTClient down to the HTTP layer and check the URL."""
        client = RESTClient(api_key="test-key", retries=0)
        source = MassiveDataSource("test-key", PriceCache(), client=client)
        with patch.object(client, "_get", return_value=[]) as get:
            await source.start(["AAPL", "MSFT"])
        await source.stop()
        kwargs = get.call_args.kwargs
        assert kwargs["path"] == "/v2/snapshot/locale/us/markets/stocks/tickers"
        assert kwargs["params"]["tickers"] == "AAPL,MSFT"

    def test_default_client_disables_urllib3_retries(self):
        source = MassiveDataSource("test-key", PriceCache())
        assert source._client.retries == 0

    def test_min_spacing_defaults(self):
        free, _ = make_source(poll_interval=15)
        paid, _ = make_source(poll_interval=2)
        assert free._min_spacing == 12.0
        assert paid._min_spacing == 2.0
