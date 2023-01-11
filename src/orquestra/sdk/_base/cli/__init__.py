################################################################################
# © Copyright 2021-2022 Zapata Computing Inc.
################################################################################

# We wanna keep this file as lightweight as possible to minimize CLI latency.
# That's the reason why we defer the 'import's as much as possible.
#
# We could just change the entrypoints in setup.py to the new locations, but it
# would require the users to uninstall and re-install the pip package. That's
# annoying.


def main_cli():
    """
    The main CLI entrypoint.
    """
    from ._corq import _main

    _main.main_cli()


def make_v2_parser():
    from ._corq import _main

    return _main.make_v2_parser()
