import datetime
from typing import List, Literal

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from common.data_feed.price_volume_data_feed import AsyncPriceVolumeDataFeed, PriceVolumeDataFeed
from common.symbol_info import SymbolInfo

from common.data_feed.timescale.helper import _contruct_ohlcv_query


class TimeScaleDataFeed(PriceVolumeDataFeed):
    def __init__(self, db_config: dict, schema: str) -> None:
        super().__init__()
        self.db_config = db_config
        self.engine = create_engine(
            URL.create(
                "postgresql+psycopg2",
                host=db_config["host"],
                port=db_config["port"],
                username=db_config["username"],
                password=db_config["password"],
                database=db_config["database_name"],
            )
        )
        self.schema = schema
        self.index_set = set(SymbolInfo.get_index_list())

    def query_ohlcv(
        self,
        symbol: str | List[str] | None = None,
        resolution: Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"] = "day",
        start_date: str | datetime.datetime | None = None,
        end_date: str | datetime.datetime | None = None,
        ascending: bool = True,
        inclusive: Literal["both", "neither", "left", "right"] = "both",
        order_in_db: bool = False,
        return_as_dataframe: bool = True,
    ) -> pd.DataFrame:
        query = _contruct_ohlcv_query(symbol, resolution, start_date, end_date, ascending, inclusive, order_in_db)
        with self.engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {self.schema}"))
            conn.execute(text("SET TIME ZONE 'GMT'"))
            result = conn.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            if return_as_dataframe:
                res = pd.DataFrame(rows, columns=columns)
                if not order_in_db:
                    res = res.sort_values("time", ascending=ascending, ignore_index=True)
            else:
                res = rows
                if not order_in_db:
                    res = sorted(res, key=lambda x: x[0], reverse=not ascending)
        return res


class AsyncTimeScaleDataFeed(AsyncPriceVolumeDataFeed):
    def __init__(self, db_config: dict, schema: str) -> None:
        super().__init__()
        self.db_config = db_config
        self.engine = create_async_engine(
            URL.create(
                "postgresql+asyncpg",
                host=db_config["host"],
                port=db_config["port"],
                username=db_config["username"],
                password=db_config["password"],
                database=db_config["database_name"],
            )
        )
        self.schema = schema

    async def close(self):
        await self.engine.dispose()

    async def aquery_ohlcv(
        self,
        symbol: str | List[str] | None = None,
        resolution: Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"] = "day",
        start_date: str | datetime.datetime | None = None,
        end_date: str | datetime.datetime | None = None,
        ascending: bool = True,
        inclusive: Literal["both", "neither", "left", "right"] = "both",
        order_in_db: bool = False,
        return_as_dataframe: bool = True,
    ):
        query = _contruct_ohlcv_query(symbol, resolution, start_date, end_date, ascending, inclusive, order_in_db)
        async with self.engine.connect() as conn:
            await conn.execute(text(f"SET search_path TO {self.schema}"))
            await conn.execute(text("SET TIME ZONE 'GMT'"))
            result = await conn.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            if return_as_dataframe:
                res = pd.DataFrame(rows, columns=columns)
                if not order_in_db:
                    res = res.sort_values("time", ascending=ascending, ignore_index=True)
            else:
                res = rows
                if not order_in_db:
                    res = sorted(res, key=lambda x: x[0], reverse=not ascending)
        return res
