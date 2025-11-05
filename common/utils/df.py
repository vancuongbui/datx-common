from common.utils.time import convert_timezone
import datetime
import pytz
import pandas as pd
import functools
from common.utils.parallel import run_in_parallel


def convert_timezone_for_all_datetime64(
    df: pd.DataFrame, from_tz: datetime.tzinfo = pytz.UTC, to_tz: datetime.tzinfo = pytz.UTC
):
    timestamp_columns = df.select_dtypes(include=["datetime64[ns]"]).columns
    df = df.apply(
        lambda col: col.apply(lambda x: convert_timezone(x, from_tz, to_tz)) if col.name in timestamp_columns else col
    )
    return df


def convert_df_timezone(from_tz: datetime.tzinfo = pytz.UTC, to_tz: datetime.tzinfo = pytz.UTC):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            df = convert_timezone_for_all_datetime64(df, from_tz, to_tz)
            return df

        return wrapper

    return decorator


def get_df_in_parallel(func, args_list, concurrent):
    [df_list] = run_in_parallel([func], args_list, concurrent)
    not_empty_df_list = list(filter(lambda df_: not df_.empty, df_list))
    unified_df = pd.concat(not_empty_df_list, ignore_index=True)
    return unified_df
