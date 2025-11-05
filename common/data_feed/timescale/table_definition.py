import copy

from sqlalchemy import Column, DateTime, MetaData, String, Table, Double, TIMESTAMP

metadata_obj = MetaData()

STOCK_OHLCV_COLUMNS = [
    Column("time", DateTime),
    Column("symbol", String),
    Column("open", Double),
    Column("high", Double),
    Column("low", Double),
    Column("close", Double),
    Column("vol", Double),
    Column("total_vol", Double),
    Column("adj_ratio", Double),
]


STOCK_RESOLUTION_TABLE_NAME_MAP = {
    "1min": "ssi_iboard_ohlcv_1m",
    "5min": "ssi_iboard_ohlcv_5m",
    "15min": "ssi_iboard_ohlcv_15m",
    "30min": "ssi_iboard_ohlcv_30m",
    "1h": "ssi_iboard_ohlcv_1h",
    "4h": "ssi_iboard_ohlcv_4h",
    "day": "unified_ohlcv_1d",
}

STOCK_RESOLUTION_TABLE_MAP = {
    resolution: Table(
        table_name,
        metadata_obj,
        *copy.deepcopy(STOCK_OHLCV_COLUMNS),
    )
    for resolution, table_name in STOCK_RESOLUTION_TABLE_NAME_MAP.items()
}

INDEX_OHLCV_COLUMNS = [
    Column("time", TIMESTAMP(timezone=True)),
    Column("symbol", String),
    Column("open", Double),
    Column("high", Double),
    Column("low", Double),
    Column("close", Double),
    Column("vol", Double),
]


INDEX_RESOLUTION_TABLE_NAME_MAP = {
    "1min": "ssi_iboard_index_ohlcv_1m",
    "5min": "ssi_iboard_index_ohlcv_5m",
    "15min": "ssi_iboard_index_ohlcv_15m",
    "30min": "ssi_iboard_index_ohlcv_30m",
    "1h": "ssi_iboard_index_ohlcv_1h",
    "4h": "ssi_iboard_index_ohlcv_4h",
    "day": "ssi_iboard_index_ohlcv_1d",
}

INDEX_RESOLUTION_TABLE_MAP = {
    resolution: Table(
        table_name,
        metadata_obj,
        *copy.deepcopy(INDEX_OHLCV_COLUMNS),
    )
    for resolution, table_name in INDEX_RESOLUTION_TABLE_NAME_MAP.items()
}
