################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
from orquestra.workflow_shared.secrets import AuthorizationMethodStorage

from ._secrets._auth import ConfigAuthorization

AuthorizationMethodStorage.register_authorization(ConfigAuthorization)
