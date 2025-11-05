from tenacity import retry, stop_after_attempt, wait_fixed
from common.utils.httpx_helper import handle_httpx_error
from common.market_data.bsc.config import (
    BSC_URL,
    BSC_CATEGORY_ID_MAPPING,
    BSC_TRADING_VIEW_RESOLUTION_MAPPING,
    BSC_TRADING_VIEW_SPECIAL_SYMBOL_MAPPING,
)
from typing import Literal
from common.market_data.trading_view_ohlcv_api import TradingViewOHLCVAPI, UnifiedDatetimeType, ResolutionType


class BscClient(TradingViewOHLCVAPI):
    def __init__(self, timeout: float = 30) -> None:
        super().__init__(
            base_url="",
            ohlcv_endpoint=BSC_URL.TRADING_VIEW,
            default_timeout=timeout,
            resolution_mapping=BSC_TRADING_VIEW_RESOLUTION_MAPPING,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    @handle_httpx_error(reraise=True)
    def get_bsc_rec_list(self, rec_type: Literal["BSC10", "BSC30", "BSC50"]):
        response = self.client.get(BSC_URL.CATEGORY, params={"categoryId": BSC_CATEGORY_ID_MAPPING[rec_type]})
        response.raise_for_status()
        json_data = response.json()
        if json_data["s"] != "ok":
            raise Exception(f"BSC API error, status is not 'ok', response: {json_data}")
        return json_data["d"]

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    @handle_httpx_error(reraise=True)
    def get_ohlcv(
        self,
        symbol: str,
        start_date: UnifiedDatetimeType,
        end_date: UnifiedDatetimeType,
        resolution: ResolutionType,
    ):
        if symbol in BSC_TRADING_VIEW_SPECIAL_SYMBOL_MAPPING:
            special_symbol = BSC_TRADING_VIEW_SPECIAL_SYMBOL_MAPPING[symbol]
            df = super().get_ohlcv(special_symbol, start_date, end_date, resolution)
            if not df.empty:
                df = df.assign(symbol=symbol)
        else:
            df = super().get_ohlcv(symbol, start_date, end_date, resolution)
        return df
