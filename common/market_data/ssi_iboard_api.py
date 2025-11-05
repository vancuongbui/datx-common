from common.market_data.trading_view_ohlcv_api import ResolutionType, TradingViewOHLCVAPI
from common.utils.time import UnifiedDatetimeType
from common.data import HEADERS
import pandas as pd
import typing
import httpx


class SSIIboardAPI(TradingViewOHLCVAPI):
    BASE_OHLCV_URL = "https://iboard.ssi.com.vn"
    DEFAULT_HEADERS = HEADERS
    OHLCV_RESOLUTION_MAP = {"1min": "1", "day": "1D"}

    def __init__(self, default_timeout: int = 15) -> None:
        super().__init__(
            base_url=self.BASE_OHLCV_URL,
            ohlcv_endpoint="/dchart/api/history",
            default_timeout=default_timeout,
            default_headers=self.DEFAULT_HEADERS,
            resolution_mapping=self.OHLCV_RESOLUTION_MAP,
        )

    def get_ohlcv(
        self,
        symbol: str,
        start_date: UnifiedDatetimeType,
        end_date: UnifiedDatetimeType | None = None,
        resolution: ResolutionType = "day",
    ):
        # If end_date is not provided or is too far in the future, some API will return empty data
        max_end_date = pd.Timestamp.utcnow() + pd.Timedelta(hours=1)
        if not end_date or pd.to_datetime(end_date).timestamp() > max_end_date.timestamp():
            end_date = max_end_date
        return super().get_ohlcv(symbol, start_date, end_date, resolution)

    def get_latest_quote_snapshot(self, exchange: typing.Literal["HOSE", "HNX", "UPCOM"]):
        url = f"https://iboard-query.ssi.com.vn/v2/stock/exchange/{exchange.lower()}"
        headers = {**self.DEFAULT_HEADERS, "origin": "https://iboard.ssi.com.vn"}
        res = httpx.get(url=url, headers=headers, timeout=self.default_timeout)
        res.raise_for_status()
        res_body = res.json()
        code = res_body["code"]
        if code != "SUCCESS":
            raise ValueError(f"Failed to get latest quote snapshot for {exchange}, response: {res_body}")
        data = res_body["data"]
        df = pd.DataFrame(data)
        return df
