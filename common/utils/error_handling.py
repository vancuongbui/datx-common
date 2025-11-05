from typing import Type
import functools
import logging

logger = logging.getLogger(__name__)


def reraise_as(CustomException: Type[Exception], log_error: bool = False):
    """
    Decorator that catches any exception raised by the decorated function,
    logs the error if `log_error` is True, and then raises a custom exception
    of type `CustomException` with the original exception as its cause.

    Args:
      CustomException (Type[Exception]): The custom exception class to raise.
      log_error (bool, optional): Whether to log the error. Defaults to False.

    Returns:
      function: The decorated function.

    Raises:
      CustomException: The custom exception with the original exception as its cause.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_error:
                    logger.error(repr(e))
                raise CustomException(repr(e))

        return wrapper

    return decorator
