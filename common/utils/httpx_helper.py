import httpx
import logging

logger = logging.getLogger(__name__)


def handle_httpx_error(reraise=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except httpx.HTTPStatusError as e:
                # Handle HTTP status error from 400 to 599
                logger.error(f"HTTP Status Error occurred: {e}, response: '{e.response.text}'")
                if reraise:
                    raise e
            except Exception as e:
                logger.error(f"Error occurred: {e}")
                if reraise:
                    raise e

        return wrapper

    return decorator
