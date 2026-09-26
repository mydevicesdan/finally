"""SSE endpoint streaming live prices from the PriceCache."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)


def create_stream_router(
    price_cache: PriceCache,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> APIRouter:
    """Build a fresh router (no module-level state) serving GET /api/stream/prices."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request, interval, heartbeat),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames: a full snapshot whenever the cache version changes.

    Every `data:` frame is the complete set of tracked tickers, so the client
    simply replaces its map (removed tickers disappear, empty dict = empty
    watchlist). A comment frame keeps idle connections alive through proxies.
    """
    yield "retry: 1000\n\n"
    last_version = -1
    last_sent = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version, prices = price_cache.snapshot()
            now = time.monotonic()
            if version != last_version:
                last_version = version
                payload = {ticker: update.to_dict() for ticker, update in prices.items()}
                yield f"data: {json.dumps(payload)}\n\n"
                last_sent = now
            elif now - last_sent >= heartbeat:
                yield ": keep-alive\n\n"
                last_sent = now
            await asyncio.sleep(interval)
    finally:
        logger.info("SSE client disconnected: %s", client)
