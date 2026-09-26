"""Tests for ticker normalization."""

import pytest

from app.market.tickers import normalize_ticker, normalize_tickers


class TestNormalizeTicker:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("AAPL", "AAPL"),
            ("aapl", "AAPL"),
            ("  msft \n", "MSFT"),
            ("V", "V"),
            ("brk.b", "BRK.B"),
            ("BF-B", "BF-B"),
            ("GOOGL", "GOOGL"),
            ("X2", "X2"),
        ],
    )
    def test_valid(self, raw, expected):
        assert normalize_ticker(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        ["", "   ", "123", "1ABC", "AAPL;DROP", "TOOLONGTICKER", "AA PL", "A.", "$AAPL", "BRK.BBB"],
    )
    def test_invalid(self, raw):
        with pytest.raises(ValueError, match="Invalid ticker"):
            normalize_ticker(raw)


class TestNormalizeTickers:
    def test_dedupes_keeping_order(self):
        assert normalize_tickers(["msft", "AAPL", "MSFT", " aapl"]) == ["MSFT", "AAPL"]

    def test_empty(self):
        assert normalize_tickers([]) == []

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            normalize_tickers(["AAPL", "???"])
