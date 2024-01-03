################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import time
from typing import Optional, Tuple, Type


def retry(
    *,
    attempts: int,
    allowed_exceptions: Tuple[Type[Exception]],
    delay: Optional[float] = None,
):
    """A decorator useful for when a function should be retried.

    Usage::

        @retry(attempts=2, allowed_exceptions=(RuntimeError, ConnectionError))
        def my_function_that_can_fail():
            ...

    Args:
        attempts: the maximum number of times to try the function before raising
        allowed_exceptions: the exceptions that are allowed to be retried.
            All other exceptions will not cause the function to be retried.
        delay: if set, there will be a short pause (sleep) before retrying
    """

    def _inner(fn):
        def _retried(*args, **kwargs):
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if i < (attempts - 1) and isinstance(e, allowed_exceptions):
                        if delay is not None:
                            time.sleep(delay)
                        continue
                    raise

        return _retried

    return _inner
