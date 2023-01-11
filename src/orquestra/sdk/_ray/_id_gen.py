################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Tools for generating IDs for use inside Runtimes.
"""
import hashlib
import math
import uuid


def gen_short_uid(char_length: int) -> str:
    """
    Goal: an ID that's good enough, but isn't painful to copy+paste.

    Note that simply using a substring from a UUID can be buggy – some parts of
    UUIDs can be constant.

    Examples:
        >>> gen_short_uid(4)
        '2a0e'

        >>> gen_short_uid(4)
        '6e06'

        >>> gen_short_uid(5)
        'd2f26'
    """
    a_uuid = uuid.uuid4()
    n_bytes = math.ceil(char_length / 2)
    a_hash = hashlib.blake2s(a_uuid.bytes, digest_size=n_bytes)
    return a_hash.hexdigest()[:char_length]
