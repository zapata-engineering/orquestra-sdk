################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
"""
Models for accessing the Config Service API.

API spec: https://github.com/zapatacomputing/config-service/tree/main/openapi/src
"""
import pydantic

SecretName = str
SecretValue = str


class SecretNameObj(pydantic.BaseModel):
    """
    Model for
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretName.yaml

    Named 'SecretNameObj' instead of 'SecretName' to avoid clash with the field type.
    alias.
    """

    name: SecretName


class SecretValueObj(pydantic.BaseModel):
    """
    Model for
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretValue.yaml

    Named 'SecretValueObj' instead of 'SecretValue' to avoid clash with the field type.
    alias.
    """

    value: SecretValue


class SecretDefinition(pydantic.BaseModel):
    """
    Model for
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretDefinition.yaml
    """

    name: SecretName
    value: SecretValue
