################################################################################
# © Copyright 2022 Zapata Computing Inc.
################################################################################

from . import action


def add_subparsers(subparsers):

    # orq up
    up_parser = subparsers.add_parser("up")
    up_parser.set_defaults(func=action.orq_services_up)

    # orq down
    down_parser = subparsers.add_parser("down")
    down_parser.set_defaults(func=action.orq_services_down)

    # orq status
    status_parser = subparsers.add_parser("status")
    status_parser.set_defaults(func=action.orq_services_status)
