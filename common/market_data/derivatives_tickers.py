import httpx
import pandas as pd
from numpy import int64


class DerivativesTickers:
    cookies = {
        "_ga": "GA1.2.769689600.1664856714",
        "ASP.NET_SessionId": "h04crksvt4jv3axosgisjtdq",
        "__RequestVerificationToken": "qv0k94kU-1Z-opy9HoCpk-8Ew59JJQye73rnLoQfrp3Z0cHSS1cyQmeTW8XMM_wB7sBIu8HSyNss9YUK_lnb5DovyuA9ERdlgea0JLe7gbo1",
        "Theme": "Light",
        "AnonymousNotification": "",
        "language": "en-US",
        "_gid": "GA1.2.870860420.1667183880",
        "finance_viewedstock": "VN30F1M,",
        "_gat_gtag_UA_1460625_2": "1",
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://finance.vietstock.vn",
        "Referer": "https://finance.vietstock.vn/chung-khoan-phai-sinh/hop-dong-tuong-lai.htm?page=1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.24",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Microsoft Edge";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    @classmethod
    def get_raw_records(cls):
        page = 1
        all_rows = []
        while True:
            data = {
                "page": str(page),
                "pageSize": "100",
                "__RequestVerificationToken": "3D2XU_WIqUyJA0RrpqmM8zIJlkUITddtoNqoWTLsdbNRxuYDKSoPseFXarnwuMTSsI2opT3e3GYx3z0JZ119JJ7cy6Jrlc_mGZQ24djntrE1",
            }

            response = httpx.post(
                "https://finance.vietstock.vn/derivatives/getlist",
                cookies=cls.cookies,
                headers=cls.headers,
                data=data,
            )
            rows = response.json()
            if len(rows) == 0:
                break
            total = rows[-1]["Total"]
            all_rows += rows
            if len(all_rows) >= total:
                break
            page += 1
        return all_rows

    @classmethod
    def parse_date(cls, df, col_name):
        df = df.assign(**{col_name: lambda df_: df_[col_name].str.extract("(\d+)")}).assign(
            **{col_name: lambda df_: pd.to_datetime(df_[col_name].astype(int64), unit="ms") + pd.Timedelta(days=1)}
        )
        return df

    @classmethod
    def get_ticker_info(cls):
        all_rows = cls.get_raw_records()
        df = pd.DataFrame(all_rows)
        for col_name in ["PostUpDate", "MaturityMonth", "FirstTradingDate", "LastTradingDate", "LastPaymentDate"]:
            df = cls.parse_date(df, col_name)

        # fmt: off
        df = (df
            .assign(DeltaDay=lambda df_: df_.LastTradingDate - df_.FirstTradingDate)
        )
        df = df[df.DeltaDay.dt.days < 250]
        df = (df
            .loc[:, ["StockCode", "FirstTradingDate", "LastTradingDate"]]
            .rename(columns={"StockCode": "Ticker", "FirstTradingDate": "BeginDate", "LastTradingDate": "EndDate"})
            .assign(BeginDate=lambda df_: pd.to_datetime(df_.BeginDate).dt.date)
            .assign(EndDate=lambda df_: pd.to_datetime(df_.EndDate).dt.date)
        )
        ticker_info = df.to_dict(orient="records") 
        return ticker_info

    @classmethod
    def get_vn30fnm_info(cls):
        records = DerivativesTickers.get_ticker_info()
        df = pd.DataFrame(records)
        df = (
            df.sort_values("Ticker")
            .assign(DeltaDay=lambda df_: pd.to_timedelta(df_.EndDate - df_.EndDate.shift(1), unit="D").dt.days)
            .loc[lambda df_: df_.DeltaDay < 50]
            .assign(F1MEndDate=lambda df_: df_.EndDate)
            .assign(F1MStartDate=lambda df_: (df_.F1MEndDate.shift(1) + pd.Timedelta(1, "D")).fillna(df_.BeginDate))
            .assign(F2MStartDate=lambda df_: df_.F1MStartDate.shift(1))
            .assign(F2MEndDate=lambda df_: df_.F1MEndDate.shift(1))
            .drop(labels=["DeltaDay"], axis=1)
        )
        return df

    @classmethod
    def get_vn30f_tickers(cls, type):
        """
        type: `1M` or `2M`
        """
        df = cls.get_vn30fnm_info()
        records = df.loc[:, ["Ticker", f"F{type}StartDate", f"F{type}EndDate"]].dropna().to_records(index=False)
        tickers = [tuple(x) for x in records]
        return tickers
