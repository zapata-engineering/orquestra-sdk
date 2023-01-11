################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import pytest

from orquestra.sdk._ray import _id_gen


@pytest.mark.parametrize("char_length", [4, 7, 10])
def test_gen_short_uid(char_length):
    uid = _id_gen.gen_short_uid(char_length=char_length)
    assert len(uid) == char_length
    assert uid.isalnum()
