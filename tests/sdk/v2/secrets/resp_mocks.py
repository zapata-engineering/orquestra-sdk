################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Recorded HTTP response data. Extracted from the test file because this usually
takes a lot of lines. Kept as a Python file for some DRY-ness.
"""
import typing as t


def make_get_response(secret_name: str, secret_value: str):
    """
    Based on:
    https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secret.yaml#L18
    """
    return {
        "data": {
            "details": {"name": secret_name, "value": secret_value},
            "lastModified": "2022-11-23T18:58:13.86752161Z",
            "owner": "evil/emiliano.zapata@zapatacomputing.com",
        }
    }


def make_list_response(secret_names: t.Sequence[str]):
    """
    Based on:
    https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secrets.yaml#L19
    """
    return {"data": [{"name": name} for name in secret_names]}


def make_create_response(secret_name: str):
    """
    Based on:
    https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secrets.yaml#L40
    """
    return {"name": secret_name}


def make_update_response(secret_value: str):
    """
    Based on:
    https://github.com/zapatacomputing/config-service/blob/3c4a86851bbe9f8848bb27107cdcfd168603802a/openapi/src/resources/secret.yaml#L40
    """
    return {"value": secret_value}
