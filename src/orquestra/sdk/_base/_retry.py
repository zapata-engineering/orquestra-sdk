################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import time
from typing import Optional, Sequence


def retry(
    *,
    attempts: int,
    delay: Optional[float] = None,
    allowed_exceptions: Sequence[Exception]
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
