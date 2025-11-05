import pandas as pd
from common.utils.time import UnifiedDatetimeType, to_timestamp
import typing
import httpx

ResolutionType = typing.Literal["1min", "day"]


class HttpxAPI:
    def __init__(self, base_url: str, default_headers: dict | None = None, default_timeout: int = 15) -> None:
        self.default_timeout = default_timeout
        self.client = httpx.Client(base_url=base_url, headers=default_headers, timeout=default_timeout)


class TradingViewOHLCVAPI(HttpxAPI):
    def __init__(
        self,
        base_url: str,
        ohlcv_endpoint: str,
        default_headers: dict | None = None,
        default_timeout: int = 15,
        resolution_mapping: dict | None = None,
    ) -> None:
        super().__init__(base_url=base_url, default_headers=default_headers, default_timeout=default_timeout)
        self.path = ohlcv_endpoint
        self.resolution_mapping = resolution_mapping

    def get_ohlcv(
        self,
        symbol: str,
        start_date: UnifiedDatetimeType,
        end_date: UnifiedDatetimeType | None = None,
        resolution: ResolutionType = "day",
    ):
        start_ts, end_ts = to_timestamp(start_date, end_date)
        resolution = self.resolution_mapping[resolution] if self.resolution_mapping else resolution
        params = {"symbol": symbol, "from": start_ts, "to": end_ts, "resolution": resolution}
        res = self.client.get(self.path, params=params)
        res.raise_for_status()
        data = res.json()
        if data["s"] != "ok":
            return pd.DataFrame()
        df = (
            pd.DataFrame(
                {
                    "time": data["t"],
                    "open": data["o"],
                    "high": data["h"],
                    "low": data["l"],
                    "close": data["c"],
                    "volume": data["v"],
                }
            )
            .assign(
                symbol=symbol,
                time=lambda df_: pd.to_datetime(df_["time"], unit="s", utc=True),
                open=lambda df_: df_["open"].astype(float),
                high=lambda df_: df_["high"].astype(float),
                low=lambda df_: df_["low"].astype(float),
                close=lambda df_: df_["close"].astype(float),
                volume=lambda df_: df_["volume"].astype(float),
            )
            .sort_values("time")
        )
        return df
