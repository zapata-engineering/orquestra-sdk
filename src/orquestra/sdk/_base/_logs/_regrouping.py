################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""
Utilities rearranging log lines emitted by Ray into groups suitable for our user-facing
APIs.

We want to use similar rules for parsing both CE responses and local runtime logs.
"""
import re

# These regex patterns are "compiled" at module import time, but creating each one takes
# under 1us so we should be fine.

# Example:
# "/tmp/ray/session_latest/logs/worker-
# 903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.err"
WORKER_FILE_PATTERN = re.compile(r".+/worker-(\w+)-(\d+)-\d+.(\w{3})")

# Example: "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
ENV_SETUP_FILE_PATTERN = re.compile(r".+/runtime_env_setup-(\d+).log")
