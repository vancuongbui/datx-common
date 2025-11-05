import requests
import pandas as pd
from common.data import HEADERS
from common.database_connector import factory
from common.helper import create_comparison_filter, create_in_filter, create_logical_filter
import logging

INDEX_LIST = [
    "VN100",
    "VN30",
    "VNALL",
    "VNCOND",
    "VNCONS",
    "VNDIAMOND",
    "VNENE",
    "VNFIN",
    "VNFINLEAD",
    "VNFINSELECT",
    "VNHEAL",
    "VNIND",
    "VNINDEX",
    "VNIT",
    "VNMAT",
    "VNMID",
    "VNREAL",
    "VNSI",
    "VNSML",
    "VNUTI",
    "VNX50",
    "VNXALL",
    "HNX30",
    "HNXINDEX",
    "HNXUPCOMINDEX",
]

logger = logging.getLogger(__name__)


class SymbolInfo:
    DEFAULT_COLUMNS = [
        "code",
        "exchange",
        "isin",
        "company_name_vn",
        "company_name_en",
        "short_name_vn",
        "short_name_en",
        "type",
        "listed_date",
        "delisted_date",
        "status",
        "index_code",
        "tax_code",
    ]
    DEFAULT_EXCHANGE = ["HOSE", "HNX", "UPCOM"]
    DEFAULT_STATUS = ["listed"]
    DEFAULT_TYPE = ["STOCK"]
    DEFAULT_GET_SYMBOL_URL = "https://api-finfo.vndirect.com.vn/v4/stocks?size=20000"

    @classmethod
    def get_symbol_info_from_api(cls, get_symbol_url: str = DEFAULT_GET_SYMBOL_URL):
        resp = requests.get(get_symbol_url, headers=HEADERS)
        data = resp.json()["data"]

        # fmt: off
        df = (pd.DataFrame(data)
            .rename(columns={
                "floor": "exchange",
                "companyName": "company_name_vn",
                "companyNameEng": "company_name_en",
                "shortName": "short_name_vn",
                "shortNameEng": "short_name_en",
                "listedDate": "listed_date",
                "delistedDate": "delisted_date",
                "indexCode": "index_code",
                "taxCode": "tax_code",
            })
            .drop(columns=["companyId"], errors="ignore")
            .dropna(subset=["code"])
        )
        # fmt: on
        return df

    def __init__(self, db_config: dict, table_name: str = "symbol", schema_name: str = "public"):
        """Provide symbol info

        Args:
            db_config (dict): contains database_type, host, port, username, password, database_name
            table_name (str, optional): table name. Defaults to "symbol".
            schema_name (str, optional): schema name. Defaults to "public".
        """
        self.db_config = db_config
        self.table_name = table_name
        self.schema_name = schema_name

    def query_symbol_info_from_db(
        self,
        exchange: str | list[str] | None = DEFAULT_EXCHANGE,
        status: str | list[str] | None = DEFAULT_STATUS,
        type: str | list[str] | None = DEFAULT_TYPE,
        columns: list[str] | None = DEFAULT_COLUMNS,
    ):
        def create_filter(col_name, value):
            if isinstance(value, str):
                filter = create_comparison_filter(col_name, value, "=")
            elif isinstance(value, list):
                filter = create_in_filter(col_name, value)
            else:
                raise ValueError(f"Not supported type={type(value)} for `{col_name}`")

            return filter

        exchange_col = "exchange"
        status_col = "status"
        type_col = "type"

        filters = []
        if exchange is None:
            exchange_filter = None
        else:
            exchange_filter = create_filter(exchange_col, exchange)
            filters.append(exchange_filter)

        if status is None:
            status_filter = None
        else:
            status_filter = create_filter(status_col, status)
            filters.append(status_filter)

        if type is None:
            type_filter = None
        else:
            type_filter = create_filter(type_col, type)
            filters.append(type_filter)

        if columns is None:
            columns_str = "*"
        else:
            if isinstance(columns, str):
                columns = [columns]

            if exchange_filter is not None and exchange_col not in columns:
                columns.append(exchange_col)

            if status_filter is not None and status_col not in columns:
                columns.append(status_col)

            if type_filter is not None and type_col not in columns:
                columns.append(type_col)

            columns_str = ", ".join(columns)

        query = f"SELECT {columns_str} FROM {self.schema_name}.{self.table_name}"
        if len(filters) > 0:
            where_predicate = create_logical_filter(filters, "AND")
            query = f"{query} WHERE {where_predicate}"

        conn = factory.get_connector(**self.db_config)
        df = conn.query_by_sql(query)
        return df

    def get_symbol_list(
        self,
        exchange: str | list[str] | None = ["HOSE", "HNX", "UPCOM"],
        status: str | list[str] | None = ["listed"],
        type: str | list[str] | None = ["STOCK"],
    ) -> list[str]:
        df = self.query_symbol_info_from_db(exchange, status, type)
        symbol_list = df.code.tolist()
        return symbol_list

    @classmethod
    def get_index_list(cls) -> list[str]:
        return INDEX_LIST.copy()
