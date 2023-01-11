################################################################################
# © Copyright 2022-2023 Zapata Computing Inc.
################################################################################
"""
Utility for accessing secrets managed by Orquestra.

You can use it both within an Orquestra Task code and a standalone script.

The secrets values are always stored on an Orquestra server, even if you're using
this class on your local machine.

This module's behavior depends on the execution context (in a standalone REPL/script,
in a workflow on the Local Runtime, in a workflow on Orquestra Platform).

When you're using ``orquestra.sdk.secrets`` on your machine (inside a standalone script
 or with the Local Ray Runtime), this class uses cluster credentials read from your
config file (set by ``orq login``).

When ``orquestra.sdk.secrets`` is used inside a task running remotely on the Orquestra
Platform, it uses a server-side authorization mechanism handled automatically.
"""

from ._api import delete, get, list, set

__all__ = [
    "delete",
    "get",
    "list",
    "set",
]
