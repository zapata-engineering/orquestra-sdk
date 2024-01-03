################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Pydantic models for workflow entites."""

# Note for SDK developers.
#
# Files in this module/package/dir:
# * can import from under `orquestra.sdk.schema` (this package itself)
# * should _never_ import `orquestra.sdk` or from under `orquestra.sdk._client.*`.
# * should _never_ import `orquestra.sdk.ray_wrapper`
#
# Until we explicitly split pip packages, we have to rely on developer discipline.
