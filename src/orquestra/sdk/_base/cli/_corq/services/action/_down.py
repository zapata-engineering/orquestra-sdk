################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse

from orquestra.sdk._base import _services
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    ServicesStoppedResponse,
)


def orq_services_down(args: argparse.Namespace):
    """
    CLI action to stop docker-compose.
    """
    sm = _services.ServiceManager()
    sm.down()

    return ServicesStoppedResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Services shouldn't be running now",
        ),
    )
