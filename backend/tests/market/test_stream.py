"""Tests for the SSE price stream."""

import asyncio
import json

from fastapi.responses import StreamingResponse

from app.market.cache import PriceCache
from app.market.stream import create_stream_router, generate_price_events


class FakeRequest:
    """Stand-in for starlette's Request: disconnects after N polls."""

    def __init__(self, polls_before_disconnect: int):
        self.client = None
        self._left = polls_before_disconnect

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def collect(cache: PriceCache, polls: int, **kwargs) -> list[str]:
    kwargs.setdefault("interval", 0.001)
    return [f async for f in generate_price_events(cache, FakeRequest(polls), **kwargs)]


def parse(frame: str) -> dict:
    assert frame.startswith("data: ") and frame.endswith("\n\n")
    return json.loads(frame.removeprefix("data: "))


class TestGeneratePriceEvents:
    async def test_first_frame_is_retry(self):
        frames = await collect(PriceCache(), 0)
        assert frames == ["retry: 1000\n\n"]

    async def test_snapshot_frame(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0, session_open=189.0)
        cache.update("MSFT", 420.0)
        frames = await collect(cache, 1, heartbeat=99)
        data = parse(frames[1])
        assert set(data) == {"AAPL", "MSFT"}
        assert data["AAPL"]["price"] == 190.0
        assert data["AAPL"]["session_open"] == 189.0
        assert set(data["AAPL"]) >= {
            "ticker",
            "price",
            "previous_price",
            "timestamp",
            "change",
            "change_percent",
            "direction",
            "day_change",
            "day_change_percent",
        }

    async def test_no_duplicate_frames_without_changes(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        frames = await collect(cache, 5, heartbeat=99)
        assert sum(f.startswith("data:") for f in frames) == 1

    async def test_new_frame_after_change(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        frames = []
        async for f in generate_price_events(cache, FakeRequest(5), interval=0.001, heartbeat=99):
            frames.append(f)
            if len(frames) == 2:
                cache.update("AAPL", 191.0)
        data_frames = [parse(f) for f in frames if f.startswith("data:")]
        assert [d["AAPL"]["price"] for d in data_frames] == [190.0, 191.0]
        assert data_frames[1]["AAPL"]["direction"] == "up"

    async def test_empty_cache_sends_empty_snapshot(self):
        frames = await collect(PriceCache(), 1, heartbeat=99)
        assert frames[1] == "data: {}\n\n"

    async def test_removal_is_streamed(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        cache.update("MSFT", 420.0)
        frames = []
        async for f in generate_price_events(cache, FakeRequest(6), interval=0.001, heartbeat=99):
            frames.append(f)
            if len(frames) == 2:
                cache.remove("AAPL")
            elif len(frames) == 3:
                cache.remove("MSFT")
        data_frames = [parse(f) for f in frames if f.startswith("data:")]
        assert [set(d) for d in data_frames] == [{"AAPL", "MSFT"}, {"MSFT"}, set()]

    async def test_heartbeat_when_idle(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        frames = await collect(cache, 3, heartbeat=0.0)
        assert frames[2] == ": keep-alive\n\n"

    async def test_stops_on_disconnect(self):
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        gen = generate_price_events(cache, FakeRequest(2), interval=0.001, heartbeat=99)
        frames = await asyncio.wait_for(_drain(gen), timeout=1.0)
        assert frames[0] == "retry: 1000\n\n"


async def _drain(gen) -> list[str]:
    return [f async for f in gen]


class TestStreamRouter:
    def test_each_call_builds_independent_router(self):
        r1 = create_stream_router(PriceCache())
        r2 = create_stream_router(PriceCache())
        assert r1 is not r2
        assert [route.path for route in r2.routes] == ["/api/stream/prices"]

    async def test_endpoint_headers_and_first_frames(self):
        """Call the registered endpoint directly (TestClient can't end an infinite stream)."""
        cache = PriceCache()
        cache.update("AAPL", 190.0)
        router = create_stream_router(cache, interval=0.001)
        endpoint = router.routes[0].endpoint

        response = await endpoint(FakeRequest(1))
        assert isinstance(response, StreamingResponse)
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"
        frames = [f async for f in response.body_iterator]
        assert frames[0] == "retry: 1000\n\n"
        assert parse(frames[1])["AAPL"]["price"] == 190.0
