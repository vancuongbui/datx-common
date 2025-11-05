import datetime
from typing import List, Literal

from sqlalchemy import select
from common.data_feed.timescale.table_definition import STOCK_RESOLUTION_TABLE_MAP, INDEX_RESOLUTION_TABLE_MAP
from common.symbol_info import SymbolInfo
import datetime as dt

INDEX_SET = set(SymbolInfo.get_index_list())


def _contruct_ohlcv_query(
    symbol: str | List[str] | None = None,
    resolution: Literal["1min", "5min", "15min", "30min", "1h", "4h", "day"] = "day",
    start_date: str | datetime.datetime | None = None,
    end_date: str | datetime.datetime | None = None,
    ascending: bool = True,
    inclusive: Literal["both", "neither", "left", "right"] = "both",
    order_in_db: bool = True,
):
    if isinstance(symbol, str) and symbol in INDEX_SET or isinstance(symbol, list) and symbol[0] in INDEX_SET:
        ohlcv_table = INDEX_RESOLUTION_TABLE_MAP[resolution]
    else:
        ohlcv_table = STOCK_RESOLUTION_TABLE_MAP[resolution]

    where_clause = []
    if symbol is not None:
        if isinstance(symbol, str):
            where_clause.append(ohlcv_table.c.symbol == symbol)
        elif isinstance(symbol, list):
            where_clause.append(ohlcv_table.c.symbol.in_(symbol))
        else:
            raise ValueError("symbol must be str or list[str]")

    if start_date and isinstance(start_date, str):
        start_date = dt.datetime.fromisoformat(start_date)
    if end_date and isinstance(end_date, str):
        end_date = dt.datetime.fromisoformat(end_date)

    if start_date and (inclusive == "both" or inclusive == "left"):
        where_clause.append(ohlcv_table.c.time >= start_date)
    elif start_date and (inclusive == "neither" or inclusive == "right"):
        where_clause.append(ohlcv_table.c.time > start_date)

    if end_date and (inclusive == "both" or inclusive == "right"):
        where_clause.append(ohlcv_table.c.time <= end_date)
    elif end_date and (inclusive == "neither" or inclusive == "left"):
        where_clause.append(ohlcv_table.c.time < end_date)

    query = select(ohlcv_table).where(*where_clause)
    if order_in_db:
        order_by_clause = ohlcv_table.c.time.asc() if ascending else ohlcv_table.c.time.desc()
        query = query.order_by(order_by_clause)
    return query
