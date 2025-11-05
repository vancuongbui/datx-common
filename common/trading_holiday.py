from datetime import date, datetime, timedelta

from common.database_connector import factory


class TradingHoliday:
    def __init__(
        self, holiday_db_config: dict, holiday_table_name="holiday", cache_ttl: timedelta = timedelta(days=1)
    ) -> None:
        """Provide holiday list from db

        Args:
            holiday_db_config (dict): contains database_type, host, port, username, password, database_name
            holiday_table_name (str, optional): Defaults to "holiday".
            cache_ttl (timedelta, optional): TTL for caching the holiday list. Defaults to timedelta(days=1).
        """
        self.db_config = holiday_db_config
        self.conn = factory.get_connector(**holiday_db_config)
        self.holiday_table_name = holiday_table_name
        self.holiday_list = None
        self.cache_ttl = cache_ttl
        self.last_call_time = None

    def get_holiday_list(self) -> list[date]:
        """get holiday list, caching and return it

        Returns:
            list[date]
        """
        now = datetime.now()
        if not self.last_call_time or now - self.last_call_time >= self.cache_ttl:
            query = f"SELECT * FROM {self.holiday_table_name}"
            self.holiday_list = self.conn.query_by_sql(query).date.tolist()
            self.last_call_time = datetime.now()
        return self.holiday_list

    def is_business_day(self, to_check_date: datetime | date | None = date.today()) -> bool:
        if isinstance(to_check_date, datetime):
            to_check_date = to_check_date.date()
        weekday = to_check_date.weekday()
        if weekday == 5 or weekday == 6:
            return False

        self.get_holiday_list()
        return to_check_date not in self.holiday_list
