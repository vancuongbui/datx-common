from datetime import date
from enum import StrEnum

import pandas as pd

from common.database_connector import factory
from common.helper import create_comparison_filter, create_in_filter, create_logical_filter


class EventType(StrEnum):
    CASH_DIV = "CASH_DIV"
    STOCK_DIV = "STOCK_DIV"
    BONUS = "BONUS"
    RIGHTS = "RIGHTS"


class PriceAdjustment:
    def __init__(
        self,
        db_config: dict,
        price_adj_event_table: str = "price_adjustment_event",
        price_adj_ratio_table: str = "price_adjustment_ratio",
    ) -> None:
        """Provide price adjustment data

        Args:
            db_config (dict): contains database_type, host, port, username, password, database_name
            price_adj_event_table (str, optional): Defaults to "price_adjustment_event".
            price_adj_ratio_table (str, optional): Defaults to "price_adjustment_ratio".
        """
        self.db_config = db_config
        self.price_adj_event_table = price_adj_event_table
        self.price_adj_ratio_table = price_adj_ratio_table
        self.conn = factory.get_connector(**db_config)

    def get_events(
        self,
        symbols: list[str] | None = None,
        event_types: list[EventType] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Get price adjustment events

        Args:
            symbols (list[str] | None, optional): list of symbols. Defaults to None, get all symbols.
            event_types (list[EventType] | None, optional): list of event types.  Defaults to None, get all events.
            start_date (date | None, optional): Defaults to None.
            end_date (date | None, optional): Defaults to None.

        Returns:
            pd.DataFrame
        """
        symbol_col = "symbol"
        event_type_col = "event_type"
        ex_rights_date_col = "ex_rights_date"
        where_predicates = []
        if symbols and len(symbols) != 0:
            where_predicates.append(create_in_filter(symbol_col, symbols))
        if event_types and len(event_types) != 0:
            where_predicates.append(create_in_filter(event_type_col, event_types))
        if start_date:
            where_predicates.append(create_comparison_filter(ex_rights_date_col, start_date))
        if end_date:
            where_predicates.append(create_comparison_filter(ex_rights_date_col, end_date))
        where_clause = f"WHERE {create_logical_filter(where_predicates, 'AND')}" if len(where_predicates) != 0 else ""
        query = f"""
          SELECT *
          FROM {self.price_adj_event_table}
          {where_clause}
        """

        df = self.conn.query_by_sql(query)
        return df

    def get_ratios(
        self, symbols: list[str] | None = None, start_date: date | None = None, end_date: date | None = None
    ) -> pd.DataFrame:
        """Get price adjustment ratios

        Args:
            symbols (list[str] | None, optional): list of symbols. Defaults to None, get all symbols.
            start_date (date | None, optional): Defaults to None.
            end_date (date | None, optional): Defaults to None.

        Returns:
            pd.DataFrame
        """
        symbol_col = "symbol"
        ex_rights_date_col = "ex_rights_date"
        where_predicates = []
        if symbols and len(symbols) != 0:
            where_predicates.append(create_in_filter(symbol_col, symbols))
        if start_date:
            where_predicates.append(create_comparison_filter(ex_rights_date_col, start_date))
        if end_date:
            where_predicates.append(create_comparison_filter(ex_rights_date_col, end_date))
        where_clause = f"WHERE {create_logical_filter(where_predicates, 'AND')}" if len(where_predicates) != 0 else ""
        query = f"""
          SELECT *
          FROM {self.price_adj_ratio_table}
          {where_clause}
        """

        df = self.conn.query_by_sql(query)
        return df
