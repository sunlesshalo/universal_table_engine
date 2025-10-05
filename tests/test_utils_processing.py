from __future__ import annotations

import math

import pandas as pd
import pytest

from universal_table_engine.utils import dates, numbers, pii


def test_coerce_numeric_series_handles_currency_thousands_and_percent():
    series = pd.Series(["1.234,56 lei", "12,345.67", "10%", ""])
    result = numbers.coerce_numeric_series(series)
    assert result.iloc[0] == pytest.approx(1234.56, rel=1e-6)
    assert result.iloc[1] == pytest.approx(12345.67, rel=1e-6)
    assert result.iloc[2] == pytest.approx(0.1, rel=1e-6)
    assert math.isnan(result.iloc[3])


def test_coerce_date_series_handles_digit_formats():
    series = pd.Series(["31012024", "1022024", "01/02/2024", ""])
    result = dates.coerce_date_series(series)
    iso = [value.isoformat() if not pd.isna(value) else None for value in result]
    assert iso[0] == "2024-01-31T00:00:00"
    assert iso[1] == "2024-02-01T00:00:00"
    assert iso[2].startswith("2024-02-01T")
    assert iso[3] is None


def test_iso_dates_do_not_trigger_phone_detection():
    df = pd.DataFrame({
        "iso_string": ["2024-01-31T00:00:00", "2024-02-01"],
        "datetime_col": pd.to_datetime(["2024-01-31", "2024-02-01"]),
    })
    flags = pii.detect_pii_frame(df)
    assert flags["phone"] is False


def test_phone_detection_and_masking():
    text = "Reach me at +40 371 234 567 or john.doe@example.com"
    df = pd.DataFrame({"notes": [text]})
    flags = pii.detect_pii_frame(df)
    assert flags["phone"] is True
    masked = pii.maybe_mask_value(text, mask_email_flag=True, mask_phone_flag=True)
    assert "@example.com" in masked
    assert "234" not in masked
    assert "67" in masked


def test_decimal_hint_comma():
    series_dot = pd.Series(["1.234", "5.678"])
    dot_result = numbers.coerce_numeric_series(series_dot, decimal_hint="dot")
    assert dot_result.iloc[0] == 1.234
    series_comma = pd.Series(["1.234,50", "2.345,60"])
    comma_result = numbers.coerce_numeric_series(series_comma, decimal_hint="comma")
    assert comma_result.iloc[0] == 1234.50
