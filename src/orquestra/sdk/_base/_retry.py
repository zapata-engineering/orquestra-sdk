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
