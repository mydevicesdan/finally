"""Tests for PriceUpdate dataclass."""

import pytest

from app.market.models import PriceUpdate


def make(price: float, previous: float, session_open: float = 100.0) -> PriceUpdate:
    return PriceUpdate(
        ticker="AAPL",
        price=price,
        previous_price=previous,
        timestamp=1234567890.0,
        session_open=session_open,
    )


class TestPriceUpdate:
    """Unit tests for the PriceUpdate model."""

    def test_price_update_creation(self):
        update = make(190.50, 190.00, session_open=189.00)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert update.previous_price == 190.00
        assert update.timestamp == 1234567890.0
        assert update.session_open == 189.00

    def test_change_calculation(self):
        assert make(190.50, 190.00).change == 0.50

    def test_change_negative(self):
        assert make(189.50, 190.00).change == -0.50

    def test_change_percent_up(self):
        assert make(190.00, 100.00).change_percent == 90.0

    def test_change_percent_down(self):
        assert make(100.00, 200.00).change_percent == -50.0

    def test_change_percent_zero_previous(self):
        assert make(100.00, 0.00).change_percent == 0.0

    def test_direction_up(self):
        assert make(191.00, 190.00).direction == "up"

    def test_direction_down(self):
        assert make(189.00, 190.00).direction == "down"

    def test_direction_flat(self):
        assert make(190.00, 190.00).direction == "flat"

    def test_day_change(self):
        update = make(190.42, 190.38, session_open=189.17)
        assert update.day_change == 1.25
        assert update.day_change_percent == 0.6608

    def test_day_change_negative(self):
        update = make(90.0, 91.0, session_open=100.0)
        assert update.day_change == -10.0
        assert update.day_change_percent == -10.0

    def test_day_change_percent_zero_session_open(self):
        assert make(100.0, 100.0, session_open=0.0).day_change_percent == 0.0

    def test_to_dict(self):
        result = make(190.50, 190.00, session_open=189.00).to_dict()
        assert result == {
            "ticker": "AAPL",
            "price": 190.50,
            "previous_price": 190.00,
            "timestamp": 1234567890.0,
            "change": 0.50,
            "change_percent": 0.2632,  # 0.50 / 190.00 * 100
            "direction": "up",
            "session_open": 189.00,
            "day_change": 1.50,
            "day_change_percent": 0.7937,  # 1.50 / 189.00 * 100
        }

    def test_immutability(self):
        update = make(190.50, 190.00)
        with pytest.raises(AttributeError):
            update.price = 200.00  # type: ignore[misc]
