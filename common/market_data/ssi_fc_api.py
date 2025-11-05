import datetime
import logging
import time

import requests
from tenacity import retry, retry_if_exception_type, wait_exponential

logger = logging.getLogger(__name__)


class AuthException(Exception):
    pass


class SSIFCDataAPI:
    def __init__(self, consumer_id, consumer_secret):
        self.consumer_id = consumer_id
        self.consumer_secret = consumer_secret

        self.root_url = "https://fc-data.ssi.com.vn/api/v2/Market"
        self.access_token = None

        self.page_size = 9000
        self.request_delay = 2
        self.max_wait_secs = 120

        self.retry_wrapper = retry(
            wait=wait_exponential(multiplier=self.request_delay, max=self.max_wait_secs),
            retry=retry_if_exception_type(AuthException),
            before=self.get_access_token(),
        )

        self.get_access_token()

    def get_access_token(self):
        url = f"{self.root_url}/AccessToken"
        json_data = {
            "consumerID": self.consumer_id,
            "consumerSecret": self.consumer_secret,
        }
        response = requests.post(url, json=json_data)
        if response.status_code == 200:
            self.access_token = response.json()["data"]["accessToken"]

    def get_ohlc(self, ticker, from_date, to_date, intraday=False):
        return self.retry_wrapper(SSIFCDataAPI.get_ohlc_raw)(self, ticker, from_date, to_date, intraday)

    def get_securities_details(self, market=None):
        return self.retry_wrapper(SSIFCDataAPI.get_securities_details_raw)(self, market)

    def get_securities(self, market=None):
        return self.retry_wrapper(SSIFCDataAPI.get_securities_raw)(self, market)

    def get_ohlc_raw(self, ticker, from_date, to_date, intraday):
        url = f"{self.root_url}/IntradayOhlc" if intraday else f"{self.root_url}/DailyOhlc"

        headers = {"Authorization": f"Bearer {self.access_token}"}
        now = datetime.date.today() - datetime.timedelta(days=1)
        to_date = now if to_date > now else to_date

        all_records = []
        should_stop = False

        offset = 0
        interval = 20
        while not should_stop:
            start = from_date + datetime.timedelta(offset)
            end = from_date + datetime.timedelta(offset + interval)

            if end >= to_date:
                end = to_date
                should_stop = True

            offset += interval

            begin_date_str = start.strftime("%d/%m/%Y")
            end_date_str = end.strftime("%d/%m/%Y")

            all_records_in_interval = []
            index = 1
            while True:
                params = {
                    "symbol": ticker,
                    "fromDate": begin_date_str,
                    "toDate": end_date_str,
                    "pageIndex": index,
                    "pageSize": self.page_size,
                    "ascending": True,
                }
                try:
                    response = requests.get(url, params=params, headers=headers)
                except Exception as e:
                    logger.error(repr(e))
                    time.sleep(self.request_delay)
                    continue

                json_data = response.json()
                status = json_data["status"]
                if status == 401:
                    raise AuthException

                records = json_data["data"]
                if not records:
                    break

                total = json_data["totalRecord"]
                all_records_in_interval += records

                if len(all_records_in_interval) >= total:
                    break

                index += 1
                time.sleep(self.request_delay)
            all_records += all_records_in_interval
        return all_records

    def get_securities_details_raw(self, market):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.root_url}/SecuritiesDetails"

        all_records = []
        index = 1
        while True:
            params = {
                "pageIndex": index,
                "pageSize": 1000,
            }
            if market is not None:
                params["market"] = market

            try:
                response = requests.get(url, params=params, headers=headers)
            except Exception as e:
                logger.error(repr(e))
                time.sleep(self.request_delay)
                continue

            json_data = response.json()
            status = json_data["status"]
            if status == 401:
                raise AuthException

            records = json_data["data"][0]["RepeatedInfo"]
            if not records:
                break

            total = json_data["totalRecord"]
            all_records += records

            if len(all_records) >= total:
                break

            index += 1
            time.sleep(self.request_delay)
        return all_records

    def get_securities_raw(self, market):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.root_url}/Securities"

        all_records = []
        index = 1
        while True:
            params = {
                "pageIndex": index,
                "pageSize": 1000,
            }
            if market is not None:
                params["market"] = market

            try:
                response = requests.get(url, params=params, headers=headers)
            except Exception as e:
                logger.error(repr(e))
                time.sleep(self.request_delay)
                continue

            json_data = response.json()
            status = json_data["status"]
            if status == 401:
                raise AuthException

            records = json_data["data"]
            if not records:
                break

            total = json_data["totalRecord"]
            all_records += records

            if len(all_records) >= total:
                break

            index += 1
            time.sleep(self.request_delay)
        return all_records

    def get_tickers(self, market=None, type=None):
        """
        market:
            None: all exchanges
            HOSE: HOSE exchange
            HNX: HNX exhange
            UPCOM: UPCOM exchange
            DER: derivatives
        type:
            S: stock
            W: warrant
            D: bond
            FU: derivatives
            E: ETF
            U: fund
        """
        records = self.get_securities_details(market)
        if type is None:
            tickers = [record["Symbol"] for record in records]
        else:
            tickers = [record["Symbol"] for record in records if record["SecType"] == type]
        return tickers

    def get_stock_tickers(self, market=None):
        """
        market:
            None: all exchanges
            HOSE: HOSE exchange
            HNX: HNX exhange
            UPCOM: UPCOM exchange
        This API will work at weekend
        """
        records = self.get_securities(market)
        tickers = [record["Symbol"] for record in records]
        return tickers

    def get_daily_value(self, ticker: str, query_date: datetime.date, column_name: str):
        res_list = self.get_ohlc(ticker, query_date, query_date)
        if not res_list or len(res_list) == 0 or not res_list[0].get(column_name):
            return None
        else:
            return float(res_list[0][column_name])

    def get_daily_close(self, ticker: str, query_date: datetime.date) -> float:
        return self.get_daily_value(ticker, query_date, "Close")
