################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
from unittest.mock import create_autospec

import jwt
import pytest

from orquestra.sdk.shared import exceptions
from orquestra.sdk._client._base import _jwt


class TestCheckJWTWithoutSignatureValidation:
    @pytest.fixture
    def token(self):
        return "mocked_token"

    @pytest.fixture
    def mock_jwt_decode(self, monkeypatch: pytest.MonkeyPatch):
        decode = create_autospec(jwt.decode)
        monkeypatch.setattr(jwt, "decode", decode)
        return decode

    def test_ok_token(self, token, mock_jwt_decode):
        # When
        _jwt.check_jwt_without_signature_verification(token)
        # Then Expect no exception

    def test_invalid_token(self, token, mock_jwt_decode):
        # Given
        mock_jwt_decode.side_effect = jwt.DecodeError
        # When
        with pytest.raises(exceptions.InvalidTokenError):
            _jwt.check_jwt_without_signature_verification(token)

    def test_expired_token(self, token, mock_jwt_decode):
        # Given
        mock_jwt_decode.side_effect = jwt.ExpiredSignatureError
        # When
        with pytest.raises(exceptions.ExpiredTokenError):
            _jwt.check_jwt_without_signature_verification(token)
