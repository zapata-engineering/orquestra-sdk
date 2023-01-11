################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse

from orquestra.sdk._base import _services
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    ServicesStatusResponse,
)


def orq_services_status(args: argparse.Namespace):
    """
    CLI action to get status of docker-compose.
    """
    sm = _services.ServiceManager()
    ray_running = sm.is_ray_running()
    fluentbit_running = sm.is_fluentbit_running()

    if ray_running and fluentbit_running:
        return ServicesStatusResponse(
            meta=ResponseMetadata(
                success=True,
                code=ResponseStatusCode.OK,
                message="All services running",
            ),
            ray_running=ray_running,
            fluentbit_running=fluentbit_running,
        )
    else:
        messages = [
            *([] if ray_running else ["Ray isn't running."]),
            *([] if fluentbit_running else ["FluentBit isn't running."]),
        ]
        return ServicesStatusResponse(
            meta=ResponseMetadata(
                success=False,
                code=ResponseStatusCode.SERVICES_ERROR,
                message=" ".join(messages),
            ),
            ray_running=ray_running,
            fluentbit_running=fluentbit_running,
        )
