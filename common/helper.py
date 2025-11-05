import logging
import re
from datetime import date, datetime
from numbers import Integral, Number, Real
from typing import Literal

import pandas as pd
import pytz

from .logging import setup_logging

setup_logging()


def convert_to_pd_timestamp(timestamp: Number | str | datetime | None):
    if timestamp is None:
        return None

    if isinstance(timestamp, Number):
        return pd.Timestamp.fromtimestamp(float(timestamp))

    if isinstance(timestamp, (str, datetime)):
        return pd.Timestamp(timestamp)

    return timestamp


def convert_to_sql_value(value: Integral | Real | str | datetime | date):
    if isinstance(value, Integral):
        value = int(value)
    elif isinstance(value, Real):
        value = float(value)
    elif isinstance(value, str):
        value = f"'{value}'"
    elif isinstance(value, datetime):
        value = value.isoformat(sep=" ")
        value = f"'{value}'"
    elif isinstance(value, date):
        value = value.isoformat()
        value = f"'{value}'"
    else:
        raise ValueError(f"Not supported type={type(value)}")

    return str(value)


def convert_to_alphanumeric_underscore(input_str: str) -> str:
    result = re.sub(r"[^a-zA-Z0-9_]", "", input_str)
    return result


def convert_camel_to_snake(input_str: str) -> str:
    result = re.sub(r"(?<=[a-z])(?=[A-Z])", "_", input_str).lower()
    return result


def remove_leading_numbers(input_str: str) -> str:
    result = re.sub(r"^\d+", "", input_str)
    return result


def create_in_filter(col_name: str, values: list[Integral | Real | str | datetime | date], not_in: bool = False):
    operator = "NOT IN" if not_in else "IN"
    values_str = ", ".join([convert_to_sql_value(value) for value in values])
    return f"{col_name} {operator} ({values_str})"


def create_comparison_filter(
    col_name: str,
    value: Integral | Real | str | datetime | date,
    operator: Literal["=", "<", "<=", ">", ">=", "<>", "!="],
):
    return f"{col_name} {operator} {convert_to_sql_value(value)}"


def create_logical_filter(filters: list[str], operator: Literal["AND", "OR"], with_bracket: bool = False):
    operator_sep = f" {operator} "
    return operator_sep.join([f"({filter})" for filter in filters]) if with_bracket else operator_sep.join(filters)


def convert_timezone(timestamp: datetime | pd.Timestamp, from_tz: str = None, to_tz: str = "UTC"):
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None and from_tz:
            timestamp = pytz.timezone(from_tz).localize(timestamp)
        return timestamp.astimezone(pytz.timezone(to_tz)).replace(tzinfo=None)
    elif isinstance(timestamp, pd.Timestamp):
        if timestamp.tzinfo is None and from_tz:
            timestamp = timestamp.tz_localize(from_tz)
        return timestamp.tz_convert(to_tz).tz_localize(None)
    else:
        logging.error("Can not convert timestamp!")


def get_timestamp(dt: datetime, unit: Literal["s", "ms"] = "s") -> int:
    if unit == "s":
        return round(dt.timestamp())
    return round(dt.timestamp() * 1000)
