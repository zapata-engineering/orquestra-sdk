################################################################################
# © Copyright 2023 Zapata Computing Inc.
################################################################################

from unittest.mock import Mock

import pytest

from orquestra.workflow_runtime._ray._client import RayClient


class TestClient:
    @staticmethod
    @pytest.fixture
    def client():
        return RayClient()

    class TestAddOptions:
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

    class TestOutput:
        def test_retry_on_error(self, client: RayClient, monkeypatch):
            import orquestra.workflow_shared._retry
            import ray

            get_mock = Mock()
            get_mock.side_effect = [
                ray.exceptions.RaySystemError(Mock()),
                ray.exceptions.RaySystemError(Mock()),
                ray.exceptions.RaySystemError(Mock()),
                5,
            ]

            monkeypatch.setattr(ray, "get", get_mock)
            monkeypatch.setattr(
                orquestra.workflow_shared._retry.time,  # type:ignore[reportPrivateImportUsage] # noqa: E501
                "sleep",
                Mock(),
            )

            ret_val = client.get(Mock())

            assert ret_val == 5
            assert get_mock.call_count == 4

        def test_retry_on_error_always_fails(self, client: RayClient, monkeypatch):
            import orquestra.workflow_shared._retry
            import ray

            get_mock = Mock()
            get_mock.side_effect = [ray.exceptions.RaySystemError(Mock())] * 20

            monkeypatch.setattr(ray, "get", get_mock)
            monkeypatch.setattr(
                orquestra.workflow_shared._retry.time,  # type:ignore[reportPrivateImportUsage] # noqa: E501
                "sleep",
                Mock(),
            )

            with pytest.raises(ray.exceptions.RaySystemError):
                client.get(Mock())

            assert get_mock.call_count == 20
