################################################################################
# © Copyright 2024 Zapata Computing Inc.
################################################################################
# THIS FILE IS A SHIM TO REEXPORT SYMBOLS FOR BACKWARDS COMPATIBILITY
# WITH OLD WORKFLOW RESULTS.
# DO NOT PUT ANY LOGIC INTO THIS FILE.

from orquestra.workflow_runtime import TaskResult

__all__ = ["TaskResult"]
