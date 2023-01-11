################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Exception types related to the secrets management.
"""
import requests


class SecretNotFoundError(Exception):
    """
    Raised when accessing a secret that the Config Service doesn't know about.
    """

    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        super().__init__(secret_name)


class SecretAlreadyExistsError(Exception):
    """
    Raised when we want to create a secret, and there already is another one with the
    same name.
    """

    def __init__(self, secret_name: str):
        self.secret_name = secret_name
        super().__init__(secret_name)


class InvalidTokenError(Exception):
    """
    Raised when the communication with the external Config Service couldn't be made
    because of an invalid token.
    """


class UnknownHTTPError(Exception):
    """
    Raised when there's an error we don't handle otherwise.
    """

    def __init__(self, response: requests.Response):
        self.response = response
        super().__init__(response)
