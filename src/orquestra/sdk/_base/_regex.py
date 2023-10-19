################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

# From https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string  # noqa:E501
# with addition of `\.` before prerelease to match setuptools_scm format
# The regex is split into parts to make reading the expressions easier
VERSION_NUMBER = r"0|[1-9]\d*"
PRERELEASE = r"(?:{VERSION_NUMBER}|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:{VERSION_NUMBER}|\d*[a-zA-Z-][0-9a-zA-Z-]*))*"  # noqa: E501
BUILD_METADATA = r"[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*"

# VERSION_REGEX and SEMVER_REGEX are the same:
# however, SEMVER_REGEX matches an entire line and includes group names.
VERSION_REGEX = rf"({VERSION_NUMBER})\.({VERSION_NUMBER})\.({VERSION_NUMBER})(?:[-\.]({PRERELEASE}))?(?:\+({BUILD_METADATA}))?"  # noqa: E501
SEMVER_REGEX = rf"^(?P<major>{VERSION_NUMBER})\.(?P<minor>{VERSION_NUMBER})(?:\.(?P<patch>{VERSION_NUMBER}))?(?:[-\.](?P<prerelease>{PRERELEASE}))?(?:\+(?P<buildmetadata>{BUILD_METADATA}))?$"  # noqa: E501
