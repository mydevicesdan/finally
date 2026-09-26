# Market Data Backend — Code Review

**Date:** 2026-09-26
**Scope:** `backend/app/market/` (9 modules), `backend/tests/market/` (6 test modules), `backend/market_data_demo.py`, measured against `planning/PLAN.md`, `planning/MARKET_DATA_SUMMARY.md` and `planning/MARKET_DATA_DESIGN.md`.
**Code reviewed:** `main` at `3199cb5`.
**Supersedes:** `planning/archive/MARKET_DATA_REVIEW.md` (2026-02-10). All 7 issues from that review are resolved.

---

## 1. Verdict

**The simulator path is ready for the rest of the platform to build on. The Massive (real data) path doesn't work and must be fixed before anyone sets `MASSIVE_API_KEY`.**

- All 73 tests pass. Coverage is 91%. `ruff check` is clean.
- The SSE endpoint streams correctly end to end under uvicorn.
- The Massive client drops every real API response because it reads a field the SDK doesn't have (§3.1). The test suite can't see this because it builds fake snapshots with `MagicMock`.
- Beyond that there are 3 high-severity functional gaps, 4 medium and 7 low issues. Almost all are cheap to fix. Each has a ready fix in `MARKET_DATA_DESIGN.md` §2.

Every defect below was reproduced by running code, not only found by reading. §6 lists how.

---

## 2. Test, Coverage & Lint Results

```
$ uv run --extra dev pytest --cov=app --cov-report=term-missing
73 passed in 5.25s

Name                           Stmts   Miss  Cover   Missing
app/market/cache.py               39      0   100%
app/market/factory.py             15      0   100%
app/market/interface.py           13      0   100%
app/market/massive_client.py      67      4    94%   85-87, 125
app/market/models.py              26      0   100%
app/market/seed_prices.py          8      0   100%
app/market/simulator.py          139      3    98%   149, 268-269
app/market/stream.py              36     24    33%   26-48, 62-87
TOTAL                            349     31    91%
```

| Check | Result |
|---|---|
| `pytest` (73 tests) | ✅ all pass. They also pass with `-W error::DeprecationWarning`. |
| Coverage | 91% overall. `stream.py` is only 33%. `massive_client.py` shows 94%, but that figure is misleading (§4). |
| `ruff check app/ tests/ market_data_demo.py` | ✅ All checks passed |
| `ruff format --check` | ⚠️ 3 test files would be reformatted (`test_models.py`, `test_simulator.py`, `test_simulator_source.py`) |
| End-to-end SSE (uvicorn + curl) | ✅ `retry: 1000` then one full snapshot about every 500 ms (4 frames in 2 s) |
| `market_data_demo.py` | ✅ runs cleanly (4 s smoke run, no errors) |

---

## 3. Findings

Severity: **High** means wrong behaviour a user will see or a broken feature. **Medium** means wrong under realistic conditions. **Low** means robustness, hygiene or future risk.

### 3.1 [High] Massive client discards every real snapshot

`massive_client.py:294`:

```python
timestamp = snap.last_trade.timestamp / 1000.0
```

In the installed SDK (`massive` 2.2.0), `LastTrade` has no `timestamp` attribute. The trade time is `sip_timestamp`, in **nanoseconds**, not milliseconds. Each snapshot raises `AttributeError`. The `except (AttributeError, TypeError)` on line 301 catches it, and the snapshot is logged as "Skipping" and dropped.

**Reproduced:** a real `TickerSnapshot.from_dict({"ticker":"AAPL","lastTrade":{"p":190.42,"t":…}})` fed through `_poll_once()` leaves `cache.get("AAPL") is None` and logs:

```
WARNING Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'
```

**Impact:** with `MASSIVE_API_KEY` set the app starts, SSE connects and shows green, but no ticker ever gets a price. Trades on any ticker then fail.

**Fix:** parse with the real fields (`last_trade.sip_timestamp / 1e9`). Fall back to `min.close`, `day.close` and `prev_day.close` when there's no last trade (pre-market, weekends). See `parse_snapshot()` in design §10.3–10.4. The archived `MASSIVE_API.md` has the same wrong field names (`last_trade.timestamp`, `day.previous_close`, `day.change_percent`). Mark it as inaccurate or correct it.

### 3.2 [High] No daily-change reference price

PLAN §10 asks for "daily change %" in the watchlist. `PriceUpdate` has only tick-to-tick `change` / `change_percent` against the previous update. At 500 ms those values are about 0.01%, and the stream carries nothing else to compute a daily change from. The frontend can't build this column.

**Fix:** add `session_open` to `PriceUpdate` (Massive: `prev_day.close`; simulator: the price when the ticker entered the simulation). Add `day_change` and `day_change_percent` too, and include them in `to_dict()`. The change is additive, so current consumers are unaffected (design §6).

### 3.3 [High] Held positions lose their price when removed from the watchlist

`remove_ticker()` deletes the ticker from the source and the cache. Nothing in the market layer, and no documented contract, keeps tickers with open positions tracked. Once the watchlist route calls `remove_ticker()`, portfolio valuation, P&L, the heatmap and snapshots have no price for that holding.

**Fix:** track watchlist ∪ open positions via a reconciliation helper (`sync_tracked_tickers()`, design §13.3). Document the rule in `backend/CLAUDE.md` so the portfolio and watchlist agents follow it.

### 3.4 [Medium] Removed tickers never disappear from the SSE stream

`PriceCache.remove()` doesn't bump `version`. `_generate_events()` also skips empty snapshots (`if prices:`).

- A client keeps showing a removed ticker until some other ticker updates. With the simulator that's under 500 ms. With Massive it's up to 15 s.
- When the last ticker is removed, clients **never** learn: the version never changes and an empty map is never sent. **Reproduced** under uvicorn: after removing every ticker, a fresh connection gets only `retry: 1000` and no `data:` frame.

**Fix:** make `remove()` bump `version` when it actually removed something, and send `data: {}` for an empty cache.

### 3.5 [Medium] Race: an in-flight Massive poll re-inserts a removed ticker

`_poll_once()` awaits the fetch in a worker thread, then writes every returned snapshot. If `remove_ticker()` runs during the fetch, the result writes the ticker back into the cache. The source no longer tracks it, so it stays in the cache and in SSE indefinitely.

**Reproduced:** a slow fetch with `remove_ticker("AAPL")` mid-flight ends with `"AAPL" in cache` and `get_tickers() == []`.

**Fix:** after the fetch, only write tickers still in `self._tickers` (design §10.4).

### 3.6 [Medium] Massive rate-limit handling spends the free-tier quota

`RESTClient(api_key=…)` uses the default `retries=3` (verified). The SDK's urllib3 `Retry` has `429` in `status_forcelist`. One rate-limited poll can therefore make up to 4 requests against a 5-requests-per-minute budget. The poll loop has no backoff, so the next cycle 15 s later repeats the pattern. Errors are logged at ERROR every cycle, with no hint about the fix (bad key, or a plan without snapshot access).

**Fix:** use `retries=0` and let the poll loop own retries. Add exponential backoff. Classify auth and plan errors, and log them once with a remedy (design §10.5).

### 3.7 [Medium] Ticker symbols aren't normalized consistently

- `SimulatorDataSource` doesn't normalize at all. **Reproduced:** `add_ticker("aapl")` after `start(["AAPL"])` tracks `['AAPL', 'aapl']` as two instruments, each with its own random walk and cache entry.
- `MassiveDataSource.add_ticker/remove_ticker` normalize, but `start()` doesn't. **Reproduced:** `start(["aapl", "AAPL", "AAPL"])` tracks `['aapl', 'AAPL', 'AAPL']` and sends the duplicates and the lowercase symbol to the API.
- Nothing validates input, so arbitrary strings from the REST API or the LLM go into the cache and the Massive query string.

**Fix:** one `normalize_ticker()` (trim, upper-case, validate against a regex), used by both sources and by the API routes (design §5).

### 3.8 [Low] `create_stream_router()` mutates a module-level router

`stream.py:16` creates `router` at import time, and each `create_stream_router()` call adds another `/prices` route to it. **Reproduced:** two calls return the same object with routes `['/api/stream/prices', '/api/stream/prices']`, and the second is bound to the first call's cache. This will bite tests and any `create_app()` factory.

**Fix:** create the `APIRouter` inside the function.

### 3.9 [Low] Unknown tickers re-seed randomly on every restart

`GBMSimulator._add_ticker_internal()` uses `random.uniform(50, 300)` for tickers not in `SEED_PRICES`. **Reproduced:** two simulators priced `PYPL` at $51.66 and $144.19. A user who buys PYPL, then restarts the container (the documented workflow), sees a P&L swing of hundreds of percent.

**Fix:** a deterministic seed price from `crc32(symbol)` (design §9.2).

### 3.10 [Low] `SimulatorDataSource` lifecycle edge cases

- `add_ticker()` before `start()` is silently dropped, because `_sim` is `None`. **Reproduced:** `get_tickers() == []`.
- A second `start()` creates a new task and loses the old one. **Reproduced:** the old task keeps running, and `stop()` can no longer cancel it.
- The interface docstring calls the second case "undefined behavior". Raising `RuntimeError` is cheaper than debugging it.

### 3.11 [Low] Non-deterministic simulator

`GBMSimulator` uses the global `np.random` and `random` modules. Tests can't fix a seed, so statistical properties are never verified (see §4). Use a per-instance `np.random.default_rng(seed)`.

### 3.12 [Low] Cache consistency details

- `update()` uses `ts = timestamp or time.time()`, which treats a legitimate `0.0` as missing (reproduced). Use `is None`.
- The `version` property reads without the lock. SSE reads `version` and `get_all()` as two separate lock acquisitions, so it can pair an old version with newer data. This is harmless today; an atomic `snapshot()` removes the question.

### 3.13 [Low] No SSE keep-alive

If nothing changes (empty watchlist, or future long Massive intervals), the stream is silent. Proxies and load balancers (App Runner / Render, per PLAN §11) may drop idle connections. Send a `: keep-alive` comment every ~15 s.

### 3.14 [Low] Small robustness items

- `np.linalg.cholesky` is unguarded. It can't fail with the current constants, but a bad edit to `seed_prices.py` would kill the simulator loop every tick. Catch `LinAlgError` and fall back to independent moves.
- `_fetch_snapshots()` reads `self._tickers` from the worker thread while the event loop may `append` to it. Pass a copy into the thread.
- `_generate_events()` swallows `CancelledError`. Use `try/finally` for the log line so cancellation propagates.
- 3 test files aren't `ruff format`-clean.

---

## 4. Test Suite Assessment

The suite is fast (under 6 s) and well organized. Its main weakness is that it confirms the code's own assumptions instead of checking against real behaviour.

| Problem | Where | Consequence |
|---|---|---|
| Snapshots are `MagicMock`s with `last_trade.timestamp` set by hand | `test_massive.py::_make_snapshot` | Hides §3.1 entirely. 94% coverage on a module that doesn't work. **Build snapshots with `TickerSnapshot.from_dict()` on wire-shaped JSON.** |
| `test_timestamp_conversion` asserts ms→s | `test_massive.py` | Encodes the wrong unit. Real trade timestamps are ns. |
| `patch("app.market.massive_client.RESTClient")` | `test_massive.py` | Patching module globals is brittle. An injectable `client=` parameter is cleaner. |
| No SSE tests | `stream.py` 33% | §3.4 and §3.8 went unnoticed. The generator can be tested directly with a fake `Request` (design §15.4). |
| `test_exception_resilience` never raises an exception | `test_simulator_source.py` | Lines 268-269 (the `except` branch) stay uncovered. The test name overstates what it checks. |
| `test_custom_event_probability` asserts nothing | `test_simulator_source.py` | Only checks that it doesn't crash. |
| No statistical checks | `test_simulator.py` | Nobody verifies per-tick σ ≈ σ√dt, sector correlation ≈ 0.6 / 0.3, or Cholesky on a large universe. These need a seedable RNG (§3.11). |
| Timing-based assertions | `test_custom_update_interval` (`version > initial + 2` within 50 ms) | Can flake on a loaded CI runner. Prefer stepping the simulator directly or using looser bounds. |
| Duplicate assertion | `test_unknown_ticker_gets_random_seed_price` | The same `assert` appears twice. |
| No tests for normalization, duplicates, removal race, lifecycle misuse | — | §3.5, §3.7 and §3.10 went unnoticed. |

---

## 5. What's Done Well

- **Architecture.** Strategy pattern plus a single shared `PriceCache` cleanly decouples producers from consumers. Downstream code will never need to know which source is active, as PLAN §6 requires.
- **GBM implementation.** Mathematically correct: exact log-normal step, Cholesky-correlated draws, and cent rounding only on output with unrounded internal state. The σ values and `dt` give realistic intraday ranges (AAPL ≈ 1.4% σ over 6.5 h).
- **Non-blocking Massive I/O.** The synchronous SDK runs via `asyncio.to_thread`, so polls never stall SSE or API routes.
- **Cache priming.** Both sources fill the cache before `start()` returns (seed prices / immediate first poll), so the first SSE frame already has data.
- **Background-task hygiene.** Tasks are named, `stop()` is idempotent and awaits cancellation, and loop exceptions are logged rather than killing the task.
- **SSE details.** The `retry:` directive, the `X-Accel-Buffering: no` header, disconnect detection, and version-gated pushes that avoid redundant payloads are all right.
- **`PriceUpdate`.** `frozen=True, slots=True`, with derived fields as properties so they can't drift from the stored prices.
- **Docs.** `backend/CLAUDE.md` and `MARKET_DATA_SUMMARY.md` give downstream agents an accurate usage guide, apart from the gaps above.

---

## 6. How Findings Were Verified

| Finding | Evidence |
|---|---|
| 3.1 | Real `TickerSnapshot.from_dict(...)` → `_poll_once()` → cache empty. The SDK logs `'LastTrade' object has no attribute 'timestamp'`. `dataclasses.fields(LastTrade)` lists `sip_timestamp`, `participant_timestamp` and `trf_timestamp` only. |
| 3.4 | uvicorn app with the simulator. After `DELETE`ing all tickers, `curl /api/stream/prices` receives only `retry: 1000`. `cache.remove()` leaves `version` unchanged. |
| 3.5 | Slow fake fetch with `remove_ticker` mid-flight → ticker back in the cache, untracked. |
| 3.6 | `RESTClient(api_key="k").retries == 3`. `massive/rest/base.py` `Retry(status_forcelist=[413, 429, 499, 500, …])`. |
| 3.7 | Simulator `get_tickers() == ['AAPL', 'aapl']`. Massive `start()` → `['aapl', 'AAPL', 'AAPL']`. |
| 3.8 | Two `create_stream_router()` calls → same object, `/api/stream/prices` registered twice. |
| 3.9 | Two `GBMSimulator(["PYPL"])` → $51.66 vs $144.19. |
| 3.10 | `add_ticker` before `start` → `[]`. After a second `start()` the first task is still running. |
| 3.12 | `update("A", 1.0, timestamp=0.0).timestamp != 0.0`. |

---

## 7. Recommended Actions

In priority order. Items 1–4 should land before the portfolio, watchlist and chat layers are built on top.

| # | Action | Fixes | Effort |
|---|---|---|---|
| 1 | Fix Massive parsing with real SDK fields + fallbacks. Rewrite `test_massive.py` around `TickerSnapshot.from_dict`. | 3.1, §4 | S |
| 2 | Add `session_open` / `day_change` / `day_change_percent` to `PriceUpdate` and SSE | 3.2 | S |
| 3 | `remove()` bumps version. SSE sends empty snapshots and a keep-alive. Router built per call. Add generator tests. | 3.4, 3.8, 3.13 | S |
| 4 | Shared `normalize_ticker()` in both sources (including `start()`) | 3.7 | S |
| 5 | Guard against the Massive removal race. `retries=0` + backoff + error classification. Early poll on `add_ticker`. | 3.5, 3.6 | M |
| 6 | Seedable RNG, deterministic unknown-ticker prices, eager simulator, double-start guard, Cholesky fallback. Add statistical tests. | 3.9–3.11, 3.14 | S |
| 7 | Cache `timestamp is None`, atomic `snapshot()` | 3.12 | XS |
| 8 | App layer: track watchlist ∪ positions via `sync_tracked_tickers()`. Document it in `backend/CLAUDE.md`. | 3.3 | S (when the DB layer exists) |
| 9 | `ruff format`. Fix the weak or duplicate tests listed in §4. Correct `archive/MASSIVE_API.md`. | 3.14, §4 | XS |

`planning/MARKET_DATA_DESIGN.md` has complete code for items 1–8. In a scratch copy of the package that code passes 21 targeted tests covering these findings, against `massive` 2.2.0.
