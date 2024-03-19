################################################################################
# © Copyright 2022 - 2024 Zapata Computing Inc.
################################################################################
"""Models for accessing the Config Service API.

API spec: https://github.com/zapatacomputing/config-service/tree/main/openapi/src
"""
from typing import Optional

from .._base._storage import BaseModel

SecretName = str
SecretValue = str
ResourceGroup = str
WorkspaceId = str


class SecretNameObj(BaseModel):
    """
    Model for:
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretName.yaml.

    Named 'SecretNameObj' instead of 'SecretName' to avoid clash with the field type.
    alias.
    """  # noqa: D205, D212

    name: SecretName


class SecretValueObj(BaseModel):
    """
    Model for:
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretValue.yaml.

    Named 'SecretValueObj' instead of 'SecretValue' to avoid clash with the field type.
    alias.
    """  # noqa: D205, D212

    value: SecretValue


class SecretDefinition(BaseModel):
    """
    Model for:
    https://github.com/zapatacomputing/config-service/blob/3f275a52149fb2b74c6a8c01726cce4f390a1533/openapi/src/schemas/SecretDefinition.yaml.
    """  # noqa: D205, D212

    name: SecretName
    value: SecretValue
    resourceGroup: Optional[ResourceGroup] = None


class ListSecretsRequest(BaseModel):
    """
    Model for:
    https://github.com/zapatacomputing/config-service/blob/fbfc4627450bc9a460278b242738e55210e7bf03/openapi/src/parameters/query/workspace.yaml.
    """  # noqa: D205, D212

    workspace: WorkspaceId
