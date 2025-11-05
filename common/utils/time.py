import datetime
import pytz
import pandas as pd
import time
import random
import logging

logger = logging.getLogger(__name__)

UnifiedDatetimeType = str | datetime.datetime | datetime.date | pd.Timestamp


def convert_timezone(timestamp: pd.Timestamp, from_tz: datetime.tzinfo = pytz.UTC, to_tz: datetime.tzinfo = pytz.UTC):
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(from_tz)
    return timestamp.tz_convert(to_tz)


def to_timestamp(*args: UnifiedDatetimeType, millisecond=False) -> int | tuple[int]:
    result = (
        [int(pd.to_datetime(arg).timestamp()) for arg in args]
        if not millisecond
        else [int(pd.to_datetime(arg).timestamp() * 1000) for arg in args]
    )
    if len(result) == 1:
        return result[0]
    return tuple(result)


def delay_random(min_delay: float, max_delay: float, seed: int = None):
    random.seed(seed)
    delay = random.uniform(min_delay, max_delay)
    logger.info(f"Delaying for {delay:.2f} seconds")
    time.sleep(delay)


def rand_delay(min_time: float, max_time: float):
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = random.uniform(min_time, max_time)
            logger.info(f"Delaying for {delay:.2f} seconds")
            time.sleep(delay)
            return func(*args, **kwargs)

        return wrapper

    return decorator
