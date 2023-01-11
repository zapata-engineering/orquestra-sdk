################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################
from typing import Optional

from orquestra.sdk.schema.responses import ServiceResponse

from .. import _arg_resolvers
from .._ui import _presenters


class Action:
    """
    Encapsulates app-related logic for handling ``orq services down``.
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
        manage_fluent: Optional[bool],
        manage_all: Optional[bool],
    ):
        resolved_services = self._service_resolver.resolve(
            manage_ray, manage_fluent, manage_all
        )

        with self._presenter.show_progress(
            resolved_services, label="Stopping"
        ) as progress:
            for service in progress:
                service.down()

        services = [
            ServiceResponse(name=svc.name, is_running=svc.is_running(), info=None)
            for svc in resolved_services
        ]

        self._presenter.show_services(services=services)
