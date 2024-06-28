################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
import jwt
from orquestra.workflow_shared.exceptions import ExpiredTokenError, InvalidTokenError


def check_jwt_without_signature_verification(token: str):
    """Checks a token and ensures it is a JWT and it is not expired.

    Note: This DOES NOT CRYPTOGRAPHICALY VERIFY THE TOKEN.
    Only used as a sanity check when reading a token from the CLI.

    Args:
        token: the token to be checked.

    Raises:
        orquestra.sdk.exceptions.ExpiredTokenError: if the current date is after the
            token's expiry
        orquestra.sdk.exceptions.InvalidTokenError: if the token is not a JWT
    """
    try:
        _ = jwt.decode(token, options={"verify_signature": False, "verify_exp": True})
    except jwt.ExpiredSignatureError:
        raise ExpiredTokenError(
            "Auth token is expired. Try logging in again and verifying the date "
            "on your computer is correct."
        )
    except jwt.DecodeError:
        raise InvalidTokenError("Auth token is not a JWT. Try logging in again.")


def get_email_from_jwt_token(token: str):
    try:
        token_info = jwt.decode(token, options={"verify_signature": False})
    except jwt.DecodeError:
        raise InvalidTokenError("Auth token is not a JWT. Try logging in again.")

    return token_info["email"]
