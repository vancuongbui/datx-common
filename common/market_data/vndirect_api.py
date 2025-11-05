import pandas as pd
import datetime
import requests
import logging

logger = logging.getLogger(__name__)

HEADERS = {
    "content-type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla",
}

DATE_FMT = "%Y-%m-%d"


class VNDirectAPI:
    def transform_stock_raw_data(self, df):
        # fmt: off
        df = (df
            .rename(columns={
                "code": "Ticker",
                "date": "Date",
                "time": "Time",
                "floor": "Exchange",
                "type": "Type",
                "basicPrice": "Ref",
                "ceilingPrice": "Ceiling",
                "floorPrice": "Floor",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "average": "Average",
                "adOpen": "AdjOpen",
                "adHigh": "AdjHigh",
                "adLow": "AdjLow",
                "adClose": "AdjClose",
                "adAverage": "AdjAverage",
                "nmVolume": "MVolume",
                "nmValue": "MValue",
                "ptVolume": "PTVolume",
                "ptValue": "PTValue",
                "change": "Change",
                "adChange": "AdjChange",
                "pctChange": "PctChange",
            })
            .assign(Date=lambda df_: pd.to_datetime(df_.Date, format=DATE_FMT))
        )
        # fmt: on
        return df

    def get_sample_stock_price_daily(self, ticker, size=15):
        url = "https://finfo-api.vndirect.com.vn/v4/stock_prices/"
        params = {"sort": "date", "size": size, "page": 1, "q": f"code:{ticker}"}
        response = requests.get(url, params=params, headers=HEADERS)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        if df.empty:
            logger.error(f"{ticker=}, {response.status_code=}")
            return df

        df = self.transform_stock_raw_data(df)
        return df

    def get_stock_price_daily(self, ticker, start_date, end_date):
        url = "https://finfo-api.vndirect.com.vn/v4/stock_prices/"
        query = f"code:{ticker}~date:gte:{start_date}~date:lte:{end_date}"
        delta = datetime.datetime.strptime(
            end_date, DATE_FMT
        ) - datetime.datetime.strptime(start_date, DATE_FMT)
        params = {"sort": "date", "size": delta.days + 1, "page": 1, "q": query}

        response = requests.get(url, params=params, headers=HEADERS)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        if df.empty:
            logger.error(f"{ticker=}, {response.status_code=}")
            return df

        df = self.transform_stock_raw_data(df)
        return df

    def get_market_price_daily(self, ticker, start_date, end_date):
        url = "https://finfo-api.vndirect.com.vn/v4/vnmarket_prices/"
        query = f"code:{ticker}~date:gte:{start_date}~date:lte:{end_date}"
        delta = datetime.datetime.strptime(
            end_date, DATE_FMT
        ) - datetime.datetime.strptime(start_date, DATE_FMT)
        params = {"sort": "date", "size": delta.days + 1, "page": 1, "q": query}
        response = requests.get(url, params=params, headers=HEADERS)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        if df.empty:
            logger.error(f"{ticker=}, {response.status_code=}")
            return df

        # fmt: off
        df = (df
            .rename(columns={
                "code": "Ticker",
                "floor": "Exchange",
                "date": "Date",
                "time": "Time",
                "type": "Type",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "change": "Change",
                "pctChange": "PctChange",
                "accumulatedVol": "AccumulatedVolume",
                "accumulatedVal": "AccumulatedValua",
                "nmVolume": "MVolume",
                "nmValue": "MValue",
                "ptVolume": "PTVolume",
                "ptValue": "PTValue",
                "advances": "Advances",
                "declines": "declines",
                "noChange": "NoChange",
                "noTrade": "NoTrade",
                "ceilingStocks": "CeilingStocks",
                "floorStocks": "FloorStocks",
            })
            .assign(Date=lambda df_: pd.to_datetime(df_.Date, format=DATE_FMT))
        )
        # fmt: on
        return df

    def get_stock_tickers(self, fields=['code','floor']):
        url = "https://api-finfo.vndirect.com.vn/v4/stocks"
        query = "floor:HOSE,HNX,UPCOM~type:STOCK~status:listed"
        params = {"q": query, "size": 10000, "fields": ','.join(fields)}
        response = requests.get(url, params=params, headers=HEADERS)
        data = response.json()["data"]
        df = pd.DataFrame(data)
        if df.empty:
            logger.error(f"{response.status_code=}")
            return df
        return df.rename(columns={"code": "Symbol", "floor": "Exchange"})
        