################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

"""Unit tests for ``orquestra.sdk.exceptions``.

In general, exception classes shouldn't have any code apart from just data structures,
so theorerically there should be nothing to test. However, there are occasional bits
for unit testing, like checking if the exception message was generated correctly.

Before extending this file with tests, please consider extracting your logic away from
the exception class.
"""

from orquestra.sdk._shared.exceptions import RuntimeQuerySummaryError
from orquestra.sdk._shared.schema.configs import RuntimeName


class TestRuntimeQuerySummaryError:
    @staticmethod
    def test_found_in_ray():
        # Given
        wf_run_id = "wf.abcde12.hello"

        exc = RuntimeQuerySummaryError(
            wf_run_id=wf_run_id,
            not_found_runtimes=[
                RuntimeQuerySummaryError.RuntimeInfo(
                    runtime_name=RuntimeName.CE_REMOTE,
                    config_name="cluster1",
                    server_uri="https://cluster1.example.com",
                ),
            ],
            unauthorized_runtimes=[
                RuntimeQuerySummaryError.RuntimeInfo(
                    runtime_name=RuntimeName.CE_REMOTE,
                    config_name="cluster2",
                    server_uri="https1://cluster2.example.com",
                ),
            ],
            not_running_runtimes=[
                RuntimeQuerySummaryError.RuntimeInfo(
                    runtime_name=RuntimeName.RAY_LOCAL,
                    config_name="ray",
                    server_uri=None,
                ),
            ],
        )

        # When
        description = str(exc)

        # Then
        assert description == (
            "Couldn't find a config that knows about workflow run ID "
            "wf.abcde12.hello \n"
            "Configs with 'not found' response: ['cluster1'].\n"
            "Configs with 'unauthorized' response: ['cluster2'].\n"
            "Configs that weren't up: ['ray']."
        )
