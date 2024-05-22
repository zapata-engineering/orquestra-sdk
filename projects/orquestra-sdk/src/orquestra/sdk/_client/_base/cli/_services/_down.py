################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
import subprocess
from typing import Optional

from orquestra.workflow_shared.schema.responses import ServiceResponse

from .. import _arg_resolvers
from .._ui import _presenters


class Action:
    """Encapsulates app-related logic for handling ``orq services down``.

    It's the glue code that connects resolving missing arguments, reading data, and
    presenting the results back to the user.

    The module is considered part of the name, so this class should be read as
    ``_dorq._services._down.Action``.
    """

    def __init__(
        self,
        presenter=_presenters.ServicePresenter(),
        service_resolver=_arg_resolvers.ServiceResolver(),
    ):
        # arg resolvers
        self._service_resolver = service_resolver

        # text IO
        self._presenter = presenter

    def on_cmd_call(
        self,
        manage_ray: Optional[bool],
        manage_all: Optional[bool],
    ):
        resolved_services = self._service_resolver.resolve(
            manage_ray=manage_ray, manage_all=manage_all
        )

        success = True
        responses = []

        with self._presenter.progress_spinner("Stopping"):
            for service in resolved_services:
                try:
                    service.down()
                    responses.append(
                        ServiceResponse(
                            name=service.name,
                            is_running=service.is_running(),
                            info=None,
                        )
                    )
                except subprocess.CalledProcessError as e:
                    success = False
                    responses.append(
                        ServiceResponse(
                            name=service.name,
                            is_running=True,
                            info="\n".join(
                                [
                                    "command:",
                                    str(e.cmd),
                                    "stdout:",
                                    *e.stdout.decode().splitlines(),
                                    "stderr:",
                                    *e.stderr.decode().splitlines(),
                                ]
                            ),
                        )
                    )

        if success:
            self._presenter.show_services(responses)
        else:
            self._presenter.show_failure(responses)
