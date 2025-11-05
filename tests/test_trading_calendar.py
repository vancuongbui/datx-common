import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
import pandas as pd

import pytest

from common.trading_calendar import TradingCalendar


@pytest.fixture
def trading_calendar():
    return TradingCalendar(use_hard_coded_data=True)


def test_is_business_day(trading_calendar):
    # Test a weekday that is not a holiday
    test_date = datetime.date(2022, 12, 1)
    assert trading_calendar.is_business_day(test_date)

    # Test a weekend day
    test_date = datetime.date(2022, 12, 4)
    assert not trading_calendar.is_business_day(test_date)

    # Test a holiday
    test_date = datetime.date(2023, 9, 4)
    assert not trading_calendar.is_business_day(test_date)


def test_get_offset_busday(trading_calendar):
    # Test getting the next business day
    test_date = datetime.date(2023, 8, 31)
    result = trading_calendar.get_offset_busday(test_date, offset=1)
    assert result == datetime.date(2023, 9, 5)

    # Test getting the previous business day
    test_date = datetime.date(2023, 9, 5)
    result = trading_calendar.get_offset_busday(test_date, offset=-1)
    assert result == datetime.date(2023, 8, 31)

    # Test getting the business day with offset -2
    test_date = datetime.date(2023, 9, 5)
    result = trading_calendar.get_offset_busday(test_date, offset=-2)
    assert result == datetime.date(2023, 8, 30)

    # Test getting the business day with offset 2
    test_date = datetime.date(2023, 8, 30)
    result = trading_calendar.get_offset_busday(test_date, offset=2)
    assert result == datetime.date(2023, 9, 5)

    # Test getting the same business day
    test_date = datetime.date(2023, 9, 5)
    result = trading_calendar.get_offset_busday(test_date, offset=0)
    assert result == datetime.date(2023, 9, 5)

    # Test getting the same business day rolling backward
    test_date = datetime.date(2023, 9, 4)
    result = trading_calendar.get_offset_busday(test_date, offset=0, on_zero_offset_roll="backward")
    assert result == datetime.date(2023, 8, 31)

    # Test getting the same business day rolling forward
    test_date = datetime.date(2023, 9, 4)
    result = trading_calendar.get_offset_busday(test_date, offset=0, on_zero_offset_roll="forward")
    assert result == datetime.date(2023, 9, 5)


def test_get_holiday_list(trading_calendar):
    result = trading_calendar.get_holiday_list()
    assert isinstance(result, list)


def test_get_busday_diff(trading_calendar):
    # Test business day difference between two dates without including time
    start_date = datetime.date(2023, 12, 1)
    end_date = datetime.date(2023, 12, 5)
    result = trading_calendar.get_busday_diff(start_date, end_date, included_time=False)
    assert result == pd.Timedelta(days=1)

    start_date = datetime.date(2023, 12, 5)
    end_date = datetime.date(2023, 12, 7)
    result = trading_calendar.get_busday_diff(start_date, end_date, included_time=False)
    assert result == pd.Timedelta(days=1)

    # Test business day difference between two dates including time
    start_date = datetime.datetime(2023, 12, 1, 10, 30)
    end_date = datetime.datetime(2023, 12, 5, 15, 45)
    result = trading_calendar.get_busday_diff(start_date, end_date, included_time=True)
    assert result == pd.Timedelta(days=2, hours=5, minutes=15)

    start_date = datetime.datetime(2023, 12, 5, 10, 30)
    end_date = datetime.datetime(2023, 12, 7, 15, 45)
    result = trading_calendar.get_busday_diff(start_date, end_date, included_time=True)
    assert result == pd.Timedelta(days=2, hours=5, minutes=15)

    # Test business day difference between two dates including time but with the same date
    start_date = datetime.datetime(2023, 12, 1, 10, 30)
    end_date = datetime.datetime(2023, 12, 1, 13, 30)
    result = trading_calendar.get_busday_diff(start_date, end_date, included_time=True)
    assert result == pd.Timedelta(days=0, hours=3, minutes=0)
