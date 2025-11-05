import datetime
import json
import logging
import pandas as pd

import httpx
import numpy as np
from cachetools import TTLCache, cached
from typing import List, Union
import os

current_location = os.path.dirname(os.path.realpath(__file__))

TRADING_HOLIDAY_PATH = "/trading_holiday/"
COMMON_API_ENDPOINT = "https://common-api.datx.vn"
CACHED_TTL = 600
PERSISTED_HOLIDAY_LIST_FILE = current_location + "/../holiday_list.json"

logger = logging.getLogger(__name__)


class TradingCalendar:
    def __init__(self, cache_to_file: bool = True, use_hard_coded_data: bool = False) -> None:
        """
        Initializes a new instance of the TradingCalendar class.

        `cache_to_file` (bool): A boolean indicating whether to cache data to a file. \n
        If this is set to True, the cache will be stored in a file and will be used if the api is down.

        `use_harded_code_data` (bool): A boolean indicating whether to use hard coded data.
        """
        self.use_harded_code_data = use_hard_coded_data
        if self.use_harded_code_data:
            from common.trading_holiday_list import HARD_CODED_TRADING_HOLIDAY_LIST

            self.holiday_list = [datetime.date.fromisoformat(holiday) for holiday in HARD_CODED_TRADING_HOLIDAY_LIST]
        else:
            self.client = httpx.Client(base_url=COMMON_API_ENDPOINT)
            self.cache_to_file = cache_to_file

    @cached(cache=TTLCache(maxsize=1, ttl=CACHED_TTL))
    def get_holiday_list(self) -> List[datetime.date]:
        """Get holiday list, caching and return it

        Returns:
            list[datetime.date]
        """
        if self.use_harded_code_data:
            return self.holiday_list

        try:
            res = self.client.get(TRADING_HOLIDAY_PATH)
            res.raise_for_status()
            holiday_list = [datetime.date.fromisoformat(d) for d in res.json()["data"]]
            self.holiday_list = holiday_list.copy()
            if self.cache_to_file:
                self._persist_holiday_list(holiday_list)
            return holiday_list
        except Exception as e:
            if self.cache_to_file:
                logger.info(f"Failed to get holiday list from api. Reading from file. Error: {repr(e)}")
                try:
                    holiday_list = self._read_peristed_holiday_list()
                except FileNotFoundError as e:
                    logger.error("Persisted holiday list file not found.")
                    raise e
                self.holiday_list = holiday_list.copy()
                return holiday_list
            raise e

    def get_holiday_list_for_year(self, year: int) -> List[datetime.date]:
        """Get holiday list for a given year

        Args:
            year (int): The year to get the holiday list for

        Returns:
            list[datetime.date]
        """
        holiday_list = self.get_holiday_list()
        return [d for d in holiday_list if d.year == year]

    def _persist_holiday_list(self, holiday_list: List[datetime.date], file_name=PERSISTED_HOLIDAY_LIST_FILE) -> None:
        with open(file_name, "w") as f:
            json_str = json.dumps(holiday_list, default=str)
            f.write(json_str)
            logger.info("Holiday list persisted to file.")

    def _read_peristed_holiday_list(self, file_name=PERSISTED_HOLIDAY_LIST_FILE) -> List[datetime.date]:
        with open(file_name, "r") as f:
            holiday_list = json.loads(f.read())
            return [datetime.date.fromisoformat(d) for d in holiday_list]

    def is_business_day(self, to_check_date: Union[datetime.datetime, datetime.date, None] = datetime.date.today()) -> bool:
        """
        Check if a given date is a business day, i.e. not a weekend or holiday.

        Args:
            to_check_date (datetime.datetime | datetime.date | None): The date to check. Defaults to today's date.

        Returns:
            bool: True if the date is a business day, False otherwise.
        """
        if isinstance(to_check_date, datetime.datetime):
            to_check_date = to_check_date.date()
        weekday = to_check_date.weekday()
        if weekday == 5 or weekday == 6:
            return False

        self.get_holiday_list()
        return to_check_date not in self.holiday_list

    def get_offset_busday(self, date: datetime.date, offset=-1, on_zero_offset_roll="backward") -> np.datetime64:
        """
        Returns the business day offset from the given date, taking into account any holidays.

        Args:
            date (datetime.date): The date to start from.
            offset (int, optional): The number of business days to offset from the given date. Defaults to -1.
            If the offset is negative, the offset will be in the past. If the offset is positive, the offset will be in the future.
            on_zero_offset_roll (str, optional): The direction to roll if the offset is zero. Defaults to "backward".

        Returns:
            np.datetime64: The resulting date after applying the business day offset.
        """
        self.get_holiday_list()
        roll = "backward" if offset < 0 else "forward" if offset > 0 else on_zero_offset_roll
        return np.busday_offset(
            date,
            offsets=offset,
            roll=roll,
            holidays=self.holiday_list,
        )

    def get_busday_diff(
        self,
        start_date: Union[pd.Timestamp, datetime.datetime, datetime.date],
        end_date: Union[pd.Timestamp, datetime.datetime , datetime.date],
        included_time: bool = False,
    ) -> pd.Timedelta:
        """
        Calculates the business day difference between two dates.

        Args:
            start_date (pd.Timestamp | datetime.datetime | datetime.date): The start date.
            end_date (pd.Timestamp | datetime.datetime | datetime.date): The end date.
            included_time (bool, optional): Whether to include time in the calculation. Defaults to False.

        Returns:
            pd.Timedelta: The business day difference between the two dates.
        """
        self.get_holiday_list()
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        diff_without_time = pd.Timedelta(
            days=pd.bdate_range(
                start_date,
                end_date,
                freq="C",
                weekmask="1111100",
                holidays=self.holiday_list,
                inclusive="neither",
            ).size
        )
        if not included_time:
            return diff_without_time
        else:
            diff_til_eod = (start_date + pd.Timedelta(days=1)).normalize() - start_date
            diff_from_sod = end_date - end_date.normalize()
            diff_with_time = (
                diff_without_time + diff_til_eod + diff_from_sod
                if start_date.normalize() != end_date.normalize()
                else end_date - start_date
            )
            return diff_with_time

    def get_trading_days(
        self,
        start_date: Union[pd.Timestamp, datetime.datetime, datetime.date, str],
        end_date: Union[pd.Timestamp, datetime.datetime, datetime.date, str],
    ) -> List[datetime.datetime]:
        """
        Get all trading days between two dates.

        Args:
            start_date (pd.Timestamp | datetime.datetime | datetime.date): The start date.
            end_date (pd.Timestamp | datetime.datetime | datetime.date): The end date.

        Returns:
            List[datetime.datetime]: A list of trading days between the two dates.
        """
        self.get_holiday_list()
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        return (
            pd.bdate_range(
                start_date,
                end_date,
                freq="C",
                weekmask="1111100",
                holidays=self.holiday_list,
            )
            .to_pydatetime()
            .tolist()
        )
