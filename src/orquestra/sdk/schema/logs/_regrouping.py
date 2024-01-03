################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
"""Utilities grouping Ray logs into groups suitable for our user-facing APIs.

We want to use similar rules for parsing both CE responses and local runtime logs.
"""
from pathlib import Path

# Example:
# "/tmp/ray/session_latest/logs/worker-
# 903b5bfa866935b17bbd3d3a64d22c7c6b746c2d522ad5fca9cf1f6a-01000000-249.err"
WORKER_GLOB = "session_*/logs/worker*.???"

# Example: "/tmp/ray/session_latest/logs/runtime_env_setup-01000000.log"
ENV_SETUP_GLOB = "session_*/logs/runtime_env_setup-*.log"


def is_worker(path: Path) -> bool:
    return path.match(WORKER_GLOB)


def is_env_setup(path: Path) -> bool:
    return path.match(ENV_SETUP_GLOB)
