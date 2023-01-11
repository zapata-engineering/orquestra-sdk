################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################
import argparse

from orquestra.sdk._base import _services
from orquestra.sdk.schema.responses import (
    ResponseMetadata,
    ResponseStatusCode,
    ServicesNotRunningResponse,
    ServicesStartedResponse,
)


def orq_services_up(args: argparse.Namespace):
    """
    CLI action to start docker-compose in orquestra.
    """
    sm = _services.ServiceManager()
    try:
        sm.up()
    except RuntimeError:
        return ServicesNotRunningResponse(
            meta=ResponseMetadata(
                success=False,
                code=ResponseStatusCode.SERVICES_ERROR,
                message="Couldn't connect communicate with Ray or Docker engine",
            )
        )

    return ServicesStartedResponse(
        meta=ResponseMetadata(
            success=True,
            code=ResponseStatusCode.OK,
            message="Services successfully started.",
        ),
    )
