################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from unittest.mock import Mock

import pytest

from orquestra.sdk.runtime._ray._client import RayClient


class TestClient:
    class TestAddOptions:
        @staticmethod
        @pytest.fixture
        def client():
            return RayClient()

        @staticmethod
        @pytest.fixture
        def remote_fn():
            return Mock()

        @staticmethod
        @pytest.fixture
        def required_kwargs():
            return {
                "name": "mocked name",
                "metadata": {},
                "catch_exceptions": False,
                "runtime_env": None,
                "max_retries": 0,
            }

        @staticmethod
        def test_required_args(client: RayClient, remote_fn, required_kwargs):
            """
            Passes in all the required args and checks what we call Ray with.
            """
            # When
            client.add_options(remote_fn, **required_kwargs)

            # Then
            remote_fn.options.assert_called_with(
                _metadata={
                    "workflow.io/options": {
                        "task_id": required_kwargs["name"],
                        "metadata": required_kwargs["metadata"],
                        "catch_exceptions": required_kwargs["catch_exceptions"],
                    }
                },
                runtime_env=required_kwargs["runtime_env"],
                max_retries=required_kwargs["max_retries"],
            )

        @staticmethod
        @pytest.mark.parametrize(
            "kwarg_overrides,expected_overrides",
            [
                ({"num_cpus": None}, {}),
                ({"num_cpus": 2}, {"num_cpus": 2}),
                ({"num_gpus": 1}, {"num_gpus": 1}),
                ({"memory": 1}, {"memory": 1}),
                ({"resources": {"abc": 1}}, {"resources": {"abc": 1}}),
            ],
        )
        def test_ray_resources(
            client: RayClient,
            remote_fn,
            required_kwargs,
            kwarg_overrides,
            expected_overrides,
        ):
            """
            Passes in required args with overrides related to Ray resource requests.

            Note: Orquestra task resources are a different concept than Ray resources.
            The former is what user passes in to ``@sdk.task()`` or
            ``.with_resources()``.
            """
            # When
            client.add_options(remote_fn, **required_kwargs, **kwarg_overrides)

            # Then
            remote_fn.options.assert_called_with(
                _metadata={
                    "workflow.io/options": {
                        "task_id": required_kwargs["name"],
                        "metadata": required_kwargs["metadata"],
                        "catch_exceptions": required_kwargs["catch_exceptions"],
                    }
                },
                runtime_env=required_kwargs["runtime_env"],
                max_retries=required_kwargs["max_retries"],
                **expected_overrides,
            )
