import asyncio
import json
import logging
import math
import re
import time
from datetime import date, datetime, timedelta
from multiprocessing.pool import ThreadPool
from typing import Literal

import httpx
import pandas as pd

from common.helper import convert_camel_to_snake, get_timestamp
from common.utils.time import delay_random
from tenacity import retry, stop_after_attempt, wait_fixed


class VietStockApi:
    DEFAULT_VIETSTOCK_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Origin": "https://stockchart.vietstock.vn",
        "Pragma": "no-cache",
        "Referer": "https://stockchart.vietstock.vn/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",  # noqa E501
        "sec-ch-ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }
    OHLCV_ENDPOINT = "https://api.vietstock.vn/tvnew/history"
    OHLCV_RESOLUTIONS = ["1m", "3m", "5m", "15m", "30m", "45m", "1h", "2h", "3h", "4h", "1D"]
    EVENT_ENDPOINT = "https://finance.vietstock.vn/data/eventstypedata"
    EVENT_TYPE_MAP = {13: "CASH_DIV", 15: "STOCK_DIV", 14: "BONUS", 16: "RIGHTS"}

    def __init__(self, headers=DEFAULT_VIETSTOCK_HEADERS, timeout=10) -> None:
        self.client = httpx.Client(headers=headers, timeout=timeout)
        self.aclient = httpx.AsyncClient(headers=headers, timeout=timeout)

    def __del__(self) -> None:
        self.client.close()
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.aclient.aclose())
            else:
                loop.run_until_complete(self.aclient.aclose())
        except Exception:
            pass

    @staticmethod
    def resolution_mapping(
        resolution: Literal["1m", "3m", "5m", "15m", "30m", "45m", "1h", "2h", "3h", "4h", "1D"],
    ) -> str:
        if resolution not in VietStockApi.OHLCV_RESOLUTIONS:
            raise ValueError(f"Not supported resolution {resolution}")
        unit = resolution[-1]
        quant = resolution[0]
        if unit == "m":
            return quant
        if unit == "h":
            return f"{int(quant) * 60}"
        return resolution

    @staticmethod
    def event_mapping(event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"]):
        """Map event types to (eventTypeID, channelID)

        Args:
            event_type (Literal['CASH_DIV', 'STOCK_DIV', 'BONUS', 'RIGHTS', 'ALL_DIV', 'LISTING', 'MEETING']):
            CASH_DIV: chia co tuc bang tien
            STOCK_DIV: chia co tuc bang CP
            BONUS: thuong co phieu
            RIGHTS: phat hanh them
            ALL_DIV: tat ca cac su kien dividend
            LISTING: niem yet
            MEETIN: dai hoi co dong


        Returns:
            (ini, int): (eventTypeID, channelID)
        """
        if event_type == "CASH_DIV":
            return (1, 13)
        if event_type == "STOCK_DIV":
            return (1, 15)
        if event_type == "BONUS":
            return (1, 14)
        if event_type == "RIGHTS":
            return (1, 16)
        if event_type == "ALL_DIV":
            return (1, 0)
        if event_type == "LISTING":
            return (2, 0)
        if event_type == "MEETING":
            return (5, 0)
        raise ValueError(f"Unknown event type {event_type}")

    def get_ohlcv(
        self,
        symbol: str,
        resolution: Literal["1m", "3m", "5m", "15m", "30m", "45m", "1h", "2h", "3h", "4h", "1D"],
        start_date: datetime,
        end_date: datetime,
        raised_exception=True,
    ) -> pd.DataFrame:
        try:
            mapped_resolution = self.resolution_mapping(resolution)
            params = {
                "symbol": symbol,
                "resolution": mapped_resolution,
                "from": get_timestamp(start_date),
                "to": get_timestamp(end_date),
            }
            res = self.client.get(self.OHLCV_ENDPOINT, params=params)
            res.raise_for_status()
            json_res = res.json()
            df = pd.DataFrame(json_res)
            df = (
                df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
                .assign(time=lambda df_: pd.to_datetime(df_.time, unit="s"), symbol=symbol)
                .drop(columns=["s"])
            )
            return df

        except Exception as e:
            if raised_exception:
                raise e
            logging.exception(e)
            return pd.DataFrame()

    def _parse_fetch_events_params(
        self,
        symbol: str | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"],
        start_date: date | None = None,
        end_date: date | None = None,
        page: int = 1,
        page_size: int = 100,
    ):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }
        cookies = {
            "ASP.NET_SessionId": "5l2c54yfcvpo0h125pved5fx",
            "__RequestVerificationToken": "_2dDsXk-jKVZd7Gmsh8dynOPtUepEhlbnStEVK7sc0p64r4vZut6sqZNe5SAb7R3cFvPOBnlIrh9pCESgE-17t_H3UqdGUTngnlgPee-Mww1",  # noqa
        }

        (event_type_id, channel_id) = self.event_mapping(event_type)
        body = {
            "eventTypeID": event_type_id,
            "channelID": channel_id,
            "catID": -1,
            "page": page,
            "pageSize": page_size,
            "orderBy": "Date1",
            "orderDir": "DESC",
            "__RequestVerificationToken": "BUdNyLJHnv7ZPBP1mgkwZXnImDUhIPXG4p-4bUxMi31MPwcmupWa4lUtQyVuouYQ15ojCtnKjSnM1f6eAV5rJYNQYjiG1swG8AQwJR9h5mw1",  # noqa
        }
        if symbol:
            body["code"] = symbol
        if start_date:
            body["fDate"] = start_date.isoformat()
        if end_date:
            body["tDate"] = end_date.isoformat()

        return (body, headers, cookies)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def fetch_events_by_page(
        self,
        symbol: str | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"],
        start_date: date | None = None,
        end_date: date | None = None,
        page: int = 1,
        page_size: int = 100,
        raised_exception=True,
    ) -> tuple[list[dict] | None, int | None]:
        try:
            body, headers, cookies = self._parse_fetch_events_params(
                symbol, event_type, start_date, end_date, page, page_size
            )
            res = self.client.post(self.EVENT_ENDPOINT, data=body, headers=headers, cookies=cookies)
            res.raise_for_status()
            [items, [num_items]] = res.json()
            return items, num_items

        except json.JSONDecodeError:
            return None, None

        except Exception as e:
            if raised_exception:
                raise e
            logging.exception(e)
            return None, None

    async def afetch_events_by_page(
        self,
        symbol: str | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"],
        start_date: date | None = None,
        end_date: date | None = None,
        page: int = 1,
        page_size: int = 100,
        raised_exception=True,
    ):
        try:
            body, headers, cookies = self._parse_fetch_events_params(
                symbol, event_type, start_date, end_date, page, page_size
            )
            res = self.client.post(self.EVENT_ENDPOINT, data=body, headers=headers, cookies=cookies)
            res.raise_for_status()
            [items, [num_items]] = res.json()
            return items, num_items

        except json.JSONDecodeError:
            return None, None

        except Exception as e:
            if raised_exception:
                raise e
            logging.exception(e)
            return None, None

    async def afetch_all_events(
        self,
        symbol: str | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"],
        start_date: date | None = None,
        end_date: date | None = None,
        page_size: int = 100,
        concurrent_requests=10,
        raised_exception=True,
    ):
        async def _get_page(page: int):
            return await self.afetch_events_by_page(
                symbol, event_type, start_date, end_date, page, page_size, raised_exception
            )

        page = 1
        items, num_items = await _get_page(page)
        if not items:
            return None
        num_pages = math.ceil(num_items / page_size)
        pages_left = num_pages - page
        if pages_left == 0:
            return items

        page += 1
        num_iters = math.ceil(pages_left / concurrent_requests)
        result_list = items
        for i in range(0, num_iters):
            start_page = page + i * concurrent_requests
            end_page = min(start_page + concurrent_requests - 1, num_pages)

            tasks = []
            for page_index in range(start_page, end_page + 1):
                task = asyncio.create_task(_get_page(page_index))
                tasks.append(task)

            res_list = await asyncio.gather(*tasks)
            for item_list, _ in res_list:
                result_list += item_list

        return result_list

    def fetch_all_events(
        self,
        symbols: list[str] | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"] = "ALL_DIV",
        start_date: date | None = None,
        end_date: date | None = None,
        page_size: int = 100,
        concurrent_requests=2,
        delayed=True,
        min_delay=0.1,
        max_delay=0.5,
        raised_exception=True,
    ) -> list[dict]:
        def _get_page(_symbol: str, page: int) -> tuple[str, list[dict] | None, int | None]:
            items, num_items = self.fetch_events_by_page(
                _symbol, event_type, start_date, end_date, page, page_size, raised_exception
            )
            if delayed:
                delay_random(min_delay, max_delay)
            return _symbol, items, num_items

        result_list = []

        if concurrent_requests == 1:
            page = 1
            for symbol in symbols:
                _, items, num_items = _get_page(symbol, page)
                if not items:
                    continue
                result_list += items

                num_pages = math.ceil(num_items / page_size)
                pages_left = num_pages - page
                if pages_left == 0:
                    continue

                for page_index in range(2, num_pages + 1):
                    _, items, _ = _get_page(symbol, page_index)
                    if items:
                        result_list += items
        else:
            with ThreadPool(processes=concurrent_requests) as pool:
                page = 1
                init_task_args = [(symbol, page) for symbol in symbols]
                fetch_more_task_args = []
                for [symbol, items, num_items] in pool.starmap(_get_page, init_task_args):
                    if not items:
                        continue
                    result_list += items

                    num_pages = math.ceil(num_items / page_size)
                    pages_left = num_pages - page
                    if pages_left == 0:
                        continue
                    args = [(symbol, page_index) for page_index in range(2, num_pages + 1)]
                    fetch_more_task_args += args

                for [_, items, _] in pool.map(_get_page, fetch_more_task_args):
                    if items:
                        result_list += items

        return result_list

    def _parse_datetime(self, datetime_str: str):
        if not isinstance(datetime_str, str):
            return pd.NaT
        match_obj = re.match(r"/Date\((\d+)\)/", datetime_str)
        return pd.to_datetime(int(match_obj.group(1)), unit="ms") + timedelta(hours=7) if match_obj else pd.NaT

    def _parse_event_type(self, channel_id: int):
        return self.EVENT_TYPE_MAP.get(channel_id, None)

    def _parse_event_note(self, note: str):
        note = note.lower()
        note = re.sub(r"((đồng|đ|vnd)\s*\/\s*cp)", " vnd/cp", note)
        note = " ".join(note.strip().split())

        price_re = r"(\d+(,\d+)*([\.]\d+)?) vnd/cp"
        price_match = re.search(price_re, note)
        price = float(price_match.group(1).replace(",", "")) if price_match else None

        ratio_re = r"(((?P<denominator>(\d+([\.,]\d+)?))\s*:\s*(?P<numerator>(\d+([\.,]\d+)?)))|((?P<percentage>(\d+([\.,]\d+)?))\s*%))"
        matched_ratio = re.search(ratio_re, note)
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

    def parse_event_list(self, event_list: list[dict]) -> pd.DataFrame:
        if not event_list or len(event_list) == 0:
            return pd.DataFrame()
        df = pd.DataFrame(event_list)
        df.columns = [convert_camel_to_snake(col) for col in df.columns]
        df = (
            df.rename(columns={"gdkhqdate": "ex_rights_date", "ndkccdate": "record_date"})
            .drop(columns=["row"])
            .assign(
                ex_rights_date=lambda df_: df_.ex_rights_date.apply(self._parse_datetime),
                record_date=lambda df_: df_.record_date.apply(self._parse_datetime),
                time=lambda df_: df_.time.apply(self._parse_datetime),
                date_order=lambda df_: df_.date_order.apply(self._parse_datetime),
                exchange=lambda df_: df_.exchange.str.upper(),
                event_type=lambda df_: df_.channel_id.apply(self._parse_event_type),
            )
        )
        parsed_columns = df.note.apply(self._parse_event_note).apply(pd.Series)
        df = pd.concat([df, parsed_columns], axis=1)
        df = df.sort_values(["code", "ex_rights_date"], ascending=[True, False], ignore_index=True).drop_duplicates(
            subset=["event_id"], keep="first"
        )
        return df

    def get_div_event_df(
        self,
        symbols: list[str] | None,
        event_type: Literal["CASH_DIV", "STOCK_DIV", "BONUS", "RIGHTS", "ALL_DIV", "LISTING", "MEETING"] = "ALL_DIV",
        start_date: date | None = None,
        end_date: date | None = None,
        page_size: int = 100,
        concurrent_requests=2,
        delayed=True,
        min_delay=0.1,
        max_delay=0.5,
        raised_exception=True,
    ) -> pd.DataFrame:
        event_list = self.fetch_all_events(
            symbols,
            event_type,
            start_date,
            end_date,
            page_size,
            concurrent_requests,
            delayed,
            min_delay,
            max_delay,
            raised_exception,
        )
        df = self.parse_event_list(event_list)
        return df

    def get_foreign_data(
        self, symbol: Literal["VNINDEX", "VN30"], from_date: datetime | date | str, to_date: datetime | date | str
    ):
        from_date = pd.to_datetime(from_date).strftime("%Y-%m-%d")
        to_date = pd.to_datetime(to_date).strftime("%Y-%m-%d")
        url = "https://finance.vietstock.vn/data/KQGDGiaoDichNDTNNStockPaging"
        headers = {
            "Accept": "*/*",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://finance.vietstock.vn",
            "Referer": "https://finance.vietstock.vn/ket-qua-giao-dich?tab=gd-thoa-thuan-nn&exchange=4&code=-16",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        cookies = {
            "__RequestVerificationToken": "ywDdTlGvBL_mWGlL-kmDoC6wyH1yO8OJ9OiXFcDOUzOT8iR3YT3nXG3UvGawrr1JsKp45vAsNRfwByZs9rQ-G_y_JvCOEUI7FdcvKMDgndM1",
        }
        symbol_stock_id_map = {
            "VNINDEX": "-19",
            "VN30": "-16",
        }
        stock_id = symbol_stock_id_map.get(symbol)
        if not stock_id:
            raise ValueError(f"Not supported symbol {symbol}")

        data = []
        page = 1
        page_size = 100
        with httpx.Client(headers=headers, cookies=cookies, timeout=15) as client:
            while True:
                payload = f"page={page}&pageSize={page_size}&catID=4&stockID={stock_id}&fromDate={from_date}&toDate={to_date}&__RequestVerificationToken=iG4dA4SlKy6mKU64EizY1rqZ4hJeqFQRpzAxZYr76FoMwYmMX13ZCBNLvwJ_d0gkkY5J9wJiXykdIVDwW4Qk7Dem3ae-DSX6IgJraHWFb7I1"
                res = client.post(url=url, data=payload)
                res.raise_for_status()
                if not res.text:
                    break
                [_, detail_data, pagination] = res.json()
                if not detail_data:
                    break
                data += detail_data
                total_page = pagination[0]
                if total_page == page:
                    break
                page += 1

        df = pd.DataFrame(data)
        if df.empty:
            return df
        df.columns = [convert_camel_to_snake(col) for col in df.columns]
        df = df.assign(
            trading_date=lambda x: x.trading_date.apply(self._parse_datetime),
            symbol=symbol,
        ).drop(columns=["row", "tr_id"])
        return df

    def _parse_stock_status_code(self, stock_status: str):
        stock_status = stock_status.lower()
        stock_status_mapping = {
            "cảnh báo": "WARNING",
            "hạn chế": "RESTRICTED",
            "kiểm soát": "CONTROLLED",
            "đình chỉ": "SUSPENDED",
            "tạm dừng": "SUSPENDED",
            "hủy niêm yết": "DELISTED",
            "bình thường": "NORMAL",
        }
        code_list = [code for status, code in stock_status_mapping.items() if status in stock_status]
        code_list = sorted(code_list)
        return "_".join(code_list)

    def get_symbol_detail(self, symbols: list[str], delay: float = 0.5):
        url = "https://finance.vietstock.vn/company/tradinginfo"
        t = datetime.now().strftime("%Y%m%d%H%M%S")
        base_payload = f"s=1&t={t}&__RequestVerificationToken=AHvFDBYMRAMDDMLDhzPo7-rmzOuvNTW4ACgiAICWpkYvAoan5FRnAZyIZj-eE1kXqK_69ge1IQF-FcyUlkvcwhoO70K-PUjeZnJuSOLtkP41"
        headers = {
            "Accept": "*/*",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": "language=vi-VN; ASP.NET_SessionId=iizolfdp1n5w4mvufr13iijw; __RequestVerificationToken=aMtXy9KVkKt-ehPPNlfN_bf8YnKpxZUxKmT-n6XZIKhz5tdU-nQQ_pNAiihTBeOrwNXH8q3nxUwi3RQyKgKJ6jmvANkrZXqhv61vc3u3Bwc1; __gads=ID=4f186a6a49fde3ca:T=1713931262:RT=1713931262:S=ALNI_Max4XLVV0DQbXP77vM15ksc8854sg; __gpi=UID=00000df86d1fcff5:T=1713931262:RT=1713931262:S=ALNI_MZBBO2fSj9IRBjKS0AoRkl7YvIzwA; __eoi=ID=d2e762ac6e8d5431:T=1713931262:RT=1713931262:S=AA-AfjYjCp0xnkHC7j9Yp4sdgWJw; Theme=Light; _pbjs_userid_consent_data=3524755945110770; _gid=GA1.2.1301487851.1713931270; AnonymousNotification=; dable_uid=86648550.1712587081863; finance_viewedstock=ABR,; _ga_EXMM0DKVEX=GS1.1.1713931260.1.1.1713931347.34.0.0; _ga=GA1.2.1848962771.1713931261; cto_bundle=NbAOiF9hanVGZllxaDg1dW4wN3RkT1dRVyUyRlElMkZyVVBKU0dHTWRqRGFTQUtEMGRDSXNjczQyOTlNMzhhZUFUUGdPbmdTQndJSTl6alpJZHJPWk50cUMzQjRRRlozdnAzY25UWUJTbmNUNVQlMkZObkthNFRqNlQySXF3eDlhbnFJWkdDQlBoc0tPJTJGaExTTzB3SjJmamxsMFc2VURPQSUzRCUzRA; cto_bidid=33tsol9CakJOS1BPbDlpRGczckJZY3dDWGNENGVoa1dtRHh5R3hlTE00eGd1VFNuQUYxUkhlZ3RBVGxzdVl4MlJna1ZHJTJGMElzbDhsZDBwcCUyRjhsUDd6OHpNYmJudkZuciUyQk03d25lcVg5OGxtbmdyYyUzRA; cto_dna_bundle=cNjFGV9hanVGZllxaDg1dW4wN3RkT1dRVyUyRmFYendtMnVwR01hbUFnajBWZzZrVWU4eEI0enBlM1FHMUFuU1RDblRVSCUyQiUyRkJPMUp1Rzhla0tMenYzNndHdXFGUSUzRCUzRA",
            "Origin": "https://finance.vietstock.vn",
            "Referer": "https://finance.vietstock.vn/ABR-ctcp-dau-tu-nhan-hieu-viet.htm",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Chromium";v="124", "Microsoft Edge";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        res_list = []
        with httpx.Client(base_url=url, headers=headers, timeout=15) as client:
            for idx, symbol in enumerate(symbols):
                payload = f"code={symbol}&{base_payload}"
                res = client.post(url, data=payload)
                res.raise_for_status()
                try:
                    data = res.json()
                    if not data.get("StockCode"):
                        raise Exception("Empty response")
                    if not data.get("StockStatus"):
                        data["StockStatus"] = "bình thường"
                    res_list.append(data)
                    logging.info(f"Fetched symbol: {symbol}, ({idx+1}/{len(symbols)})")
                    time.sleep(delay)
                except Exception as _:
                    logging.info(f"Empty response: {symbol}, res: {res.text}")
                    continue
        df = pd.DataFrame(res_list)
        df.columns = [convert_camel_to_snake(col) for col in df.columns]
        df = (
            df.rename(
                columns={
                    "klcplh": "outstanding_shares",
                    "klcpny": "listed_shares",
                    "prior_close_price": "prev_close",
                    "ceiling_price": "ceil",
                    "floor_price": "floor",
                    "market_capital": "market_cap",
                    "highest_price": "high",
                    "lowest_price": "low",
                    "open_price": "open",
                    "last_price": "close",
                    "avr_price": "avg_price",
                    "stock_code": "symbol",
                    "stock_status": "stock_status_name",
                }
            )
            .assign(
                status_name=lambda df_: df_.status_name.str.lower(),
                stock_status_name=lambda df_: df_.stock_status_name.str.lower(),
            )
            .assign(
                trading_date=lambda df_: df_.trading_date.apply(self._parse_datetime),
                stock_status_code=lambda df_: df_.stock_status_name.apply(self._parse_stock_status_code).astype(
                    "category"
                ),
            )
        )
        return df
