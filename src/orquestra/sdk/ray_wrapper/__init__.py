################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Ray-specific SDK code.

Regular users shouldn't need to import this module. It's marked public because
server-side components depend on it.
"""

# Note for SDK developers.
#
# Files in this module/package/dir:
# * can import from `orquestra.sdk.schema`
# * can import from `orquestra.sdk.ray_wrapper` (this package itself)
# * should _never_ import `orquestra.sdk` or from under `orquestra.sdk._client.*`.
#
# Until we explicitly split pip packages, we have to rely on developer discipline.
