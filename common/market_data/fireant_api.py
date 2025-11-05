import datetime
import json
import logging
import re
import time
import typing
from multiprocessing.pool import ThreadPool

import httpx
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from common.helper import convert_camel_to_snake
from common.utils.time import delay_random

logger = logging.getLogger(__name__)


class FireAntAPI:
    HOME_PAGE_URL = "https://fireant.vn/trang-chu"
    BASE_URL = "https://api.fireant.vn"
    REST_V2_BASE_URL = "https://restv2.fireant.vn"
    EVENT_TYPE_MAP = {1: "CASH_DIV", 2: "STOCK_DIV", 3: "RIGHTS"}
    DEFAULT_HEADERS = {
        "sec-ch-ua": "",
        "accept": "application/json, text/plain, */*",
        "sec-ch-ua-mobile": "?0",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/110.0.5481.177 Safari/537.36",  # noqa E501
        "sec-ch-ua-platform": "",
        "origin": "https://fireant.vn",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US",
    }

    def __init__(self, timeout=10) -> None:
        self.access_token = self.get_access_token()
        self.headers = {**self.DEFAULT_HEADERS, "authorization": f"Bearer {self.access_token}"}
        self.timeout = timeout
        self.client = httpx.Client(headers=self.headers, timeout=self.timeout, follow_redirects=True)

    def __del__(self):
        self.client.close()

    def get_access_token(self):
        logger.info("Getting FireAnt anonymous access token")
        res = httpx.get(self.HOME_PAGE_URL, follow_redirects=True, timeout=30)
        res.raise_for_status()
        pattern = r'"accessToken"\s*:\s*"([^"]+)"'
        access_token = re.search(pattern, res.text).group(1)
        if not access_token:
            raise ValueError("Failed to get FireAnt anonymous access token")
        logger.info("Done")
        return access_token

    def default_transform(self, df):
        if not df.empty:
            # fmt: off
            df = (df
                .rename(columns={"date": "Date"})
                .assign(Date=lambda df_: pd.to_datetime(df_.Date))
            )
            # fmt: on
        return df

    def get_news(self, ticker: str, type=1, offset=0, limit=800):
        url = f"{self.BASE_URL}/posts"
        params = {"symbol": ticker, "type": type, "offset": offset, "limit": limit}
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            logger.error(e)

    def get_daily_price(self, ticker: str, start_date: str, end_date: str = None):
        if end_date is None:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")

        url = f"{self.BASE_URL}/symbols/{ticker}/historical-quotes"
        params = {"startDate": start_date, "endDate": end_date, "limit": 10000}

        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame.from_dict(data)
            df = self.default_transform(df)
            return df
        except Exception as e:
            logger.error(e)

    def get_daily_value(self, ticker: str, query_date: datetime.date, column_name: str):
        df = self.get_daily_price(ticker, query_date.isoformat(), query_date.isoformat())
        return df.iloc[0].at[column_name] if not df.empty else None

    def get_daily_close(self, ticker: str, query_date: datetime.date):
        close = self.get_daily_value(ticker, query_date, "priceClose")
        return close * 1000 if close else None

    def get_daily_adj_ratio(self, ticker: str, query_date: datetime.date):
        return self.get_daily_value(ticker, query_date, "adjRatio")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    def fetch_div_events(
        self,
        symbol: str,
        event_type: typing.Literal[0, 1, 2, 3] = 0,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
        offset: int = 0,
        limit: int = 100,
        order_by: typing.Literal[1, 2, 3] = 1,
        raised_exception=True,
    ) -> list | None:
        try:
            params = {
                "symbol": symbol,
                "orderBy": order_by,
                "type": event_type,
                "startDate": start_date,
                "endDate": end_date,
                "offset": offset,
                "limit": limit,
            }
            url = f"{self.REST_V2_BASE_URL}/events/search"
            res = self.client.get(url, params=params)
            res.raise_for_status()
            return res.json()
        except json.JSONDecodeError:
            logger.error(f"Failed to decode json response {res.text}")
            return None
        except Exception as e:
            if raised_exception:
                raise e
            logger.error(repr(e))
            return None

    def fetch_all_div_events(
        self,
        symbols: list[str],
        event_type: typing.Literal[0, 1, 2, 3] = 0,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
        limit: int = 100,
        concurrent_requests=10,
        delayed=True,
        min_delay=0.1,
        max_delay=0.5,
        raised_exception=True,
    ):
        def _fetch_events(symbol: str):
            event_list = []
            offset = 0
            while True:
                events = self.fetch_div_events(
                    symbol=symbol,
                    event_type=event_type,
                    start_date=start_date,
                    end_date=end_date,
                    offset=offset,
                    limit=limit,
                    raised_exception=raised_exception,
                )
                if not events:
                    break
                event_list += events
                offset += limit
                if len(events) < limit:
                    break
                if delayed:
                    delay_random(min_delay, max_delay)
            return event_list

        result_list = []
        args = [(symbol,) for symbol in symbols]
        if concurrent_requests == 1:
            for arg in args:
                result_list += _fetch_events(arg)
                if delayed:
                    delay_random(min_delay, max_delay)
        else:
            with ThreadPool(processes=concurrent_requests) as pool:
                for item_list in pool.map(_fetch_events, args):
                    result_list += item_list
        return result_list

    def _parse_event_type(self, event_type: int):
        return self.EVENT_TYPE_MAP.get(event_type)

    def _parse_event_title(self, title: str):
        title = title.lower()
        title = re.sub(r"((đồng|đ|vnd)\s*\/\s*cp)", " vnd/cp", title)
        title = " ".join(title.strip().split())

        price_re = r"(\d+(,\d+)*([\.]\d+)?) vnd/cp"
        price_match = re.search(price_re, title)
        price = float(price_match.group(1).replace(",", ".")) if price_match else None

        ratio_re = r"((?P<denominator>(\d+([\.,]\d+)?)|(\d+([\.,]\d+)?e[+-]\d+))\s*:\s*(?P<numerator>(\d+([\.,]\d+)?)))|((?P<percentage>(\d+([\.,]\d+)?))\s*%)"
        matched_ratio = re.search(ratio_re, title)
        matched_dict = matched_ratio.groupdict() if matched_ratio else dict()
        num = matched_dict.get("numerator")
        deno = matched_dict.get("denominator")
        percent = matched_dict.get("percentage")
        if num and deno:
            ratio = float(num.replace(",", ".")) / float(deno.replace(",", "."))
        elif percent:
            ratio = float(percent.replace(",", ".")) / 100
        else:
            ratio = None
        return {"ratio": ratio, "price": price}

    def parse_div_events(self, events: list[dict]) -> pd.DataFrame:
        if not events:
            return pd.DataFrame()
        df = pd.DataFrame(events)
        df.columns = [convert_camel_to_snake(col) for col in df.columns]
        df = df.rename(columns={"record_date": "ex_rights_date", "name": "company_name"}).assign(
            ex_rights_date=lambda df_: pd.to_datetime(df_.ex_rights_date, errors="coerce"),
            registration_date=lambda df_: pd.to_datetime(df_.registration_date, errors="coerce"),
            execution_date=lambda df_: pd.to_datetime(df_.execution_date, errors="coerce"),
            event_type=lambda df_: df_.type.apply(self._parse_event_type),
        )
        parsed_columns = df.title.apply(self._parse_event_title).apply(pd.Series)
        df = pd.concat([df, parsed_columns], axis=1).sort_values(
            ["symbol", "ex_rights_date"], ascending=[True, False], ignore_index=True
        )
        return df

    def get_div_event_df(
        self,
        symbols: list[str],
        event_type: typing.Literal[0, 1, 2, 3] = 0,
        start_date: datetime.date = None,
        end_date: datetime.date = None,
        limit: int = 100,
        concurrent_requests=10,
        delayed=True,
        min_delay=0.1,
        max_delay=0.5,
        raised_exception=True,
    ) -> pd.DataFrame:
        event_list = self.fetch_all_div_events(
            symbols,
            event_type,
            start_date,
            end_date,
            limit,
            concurrent_requests,
            delayed,
            min_delay,
            max_delay,
            raised_exception,
        )
        df = self.parse_div_events(event_list)
        return df

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    def get_symbol_fundamental(self, symbol: str):
        try:
            url = f"{self.BASE_URL}/symbols/{symbol}/fundamental"
            res = self.client.get(url)
            res.raise_for_status()
            data = res.json()
            data["symbol"] = symbol
            return data
        except Exception as e:
            logger.warn(f"Encountered error: '{e}', retrying")
            if res:
                logger.warn(f"Response status code: {res.status_code}, response text: {res.text}")
                if res.status_code == 500:
                    logger.warn("No data found for symbol, returning None")
                    return None
            raise e

    def get_symbol_fundamental_list(self, symbols: list[str], delay: float = 0.5):
        data = []
        for symbol in symbols:
            symbol_data = self.get_symbol_fundamental(symbol)
            if not symbol_data:
                continue
            data.append(self.get_symbol_fundamental(symbol))
            time.sleep(delay)

        df = pd.DataFrame(data)
        df.columns = [convert_camel_to_snake(col) for col in df.columns]
        return df
