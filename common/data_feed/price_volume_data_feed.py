import datetime
from abc import ABC, abstractmethod
from typing import List, Literal, Union

import pandas as pd


class PriceVolumeDataFeed(ABC):
    @abstractmethod
    def query_ohlcv(
        self,
        symbol: Union[str, List[str], None] = None,
        resolution: Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"] = "day",
        start_date: Union[str, datetime.datetime, None] = None,
        end_date: Union[str, datetime.datetime, None] = None,
        ascending: bool = True,
        inclusive: Literal["both", "neither", "left", "right"] = "both",
        order_in_db: bool = False,
        return_as_dataframe: bool = True,
    ) -> pd.DataFrame:
        """
        Synchronously query OHLCV data for a given symbol(s) and time range.

        Args:
            symbol (Union[str, List[str], None], optional): The symbol(s) to retrieve data for. Defaults to None.
            resolution (Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"], optional): The resolution of the data to retrieve. Defaults to "day".
            start_date (Union[str, datetime.datetime, None], optional): The start date of the data to retrieve. Defaults to None.
            end_date (Union[str, datetime.datetime, None], optional): The end date of the data to retrieve. Defaults to None.
            ascending (bool, optional): Whether to sort the data in ascending order. Defaults to True.
            inclusive (Literal["both", "neither", "left", "right"], optional): Whether to include the start and end dates in the retrieved data. Defaults to "both".

        Returns:
            pd.DataFrame: A list of dictionaries containing OHLCV data for the specified symbol(s) and time range.
        """
        pass


class AsyncPriceVolumeDataFeed(ABC):
    @abstractmethod
    async def aquery_ohlcv(
        self,
        symbol: Union[str, List[str], None] = None,
        resolution: Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"] = "day",
        start_date: Union[str, datetime.datetime, None] = None,
        end_date: Union[str, datetime.datetime, None] = None,
        ascending: bool = True,
        inclusive: Literal["both", "neither", "left", "right"] = "both",
        order_in_db: bool = False,
        return_as_dataframe: bool = True,
    ):
        """
        Asynchronously query OHLCV data for a given symbol(s) and time range.

        Args:
            symbol (Union[str, List[str], None], optional): The symbol(s) to query. Defaults to None.
            resolution (Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"], optional): The resolution of the data to query. Defaults to "day".
            start_date (Union[str, datetime.datetime, None], optional): The start date of the time range to query. Defaults to None.
            end_date (Union[str, datetime.datetime, None], optional): The end date of the time range to query. Defaults to None.
            ascending (bool, optional): Whether to sort the data in ascending order. Defaults to True.
            inclusive (Literal["both", "neither", "left", "right"], optional): Whether to include the start and end dates in the query. Defaults to "both".

        Returns:
            pd.DataFrame: A pandas DataFrame containing the queried OHLCV data.
        """
        pass

    @abstractmethod
    async def close(self):
        """
        Closes the asynchronous price volume data feed.
        """
        pass
