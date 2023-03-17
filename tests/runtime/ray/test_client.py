from unittest.mock import Mock

import pytest

from orquestra.sdk._ray._client import RayClient


class TestClient:
    class TestAddOptions:
        @pytest.fixture
        def client(self):
            return RayClient()

        @pytest.fixture
        def remote_fn(self):
            return Mock()

        @pytest.fixture
        def required_kwargs(self):
            return {
                "name": "mocked name",
                "metadata": {},
                "catch_exceptions": False,
                "runtime_env": None,
            }

        def test_required_args(self, client: RayClient, remote_fn, required_kwargs):
            client.add_options(remote_fn, **required_kwargs)
            remote_fn.options.assert_called_with(
                _metadata={
                    "workflow.io/options": {
                        "task_id": required_kwargs["name"],
                        "metadata": required_kwargs["metadata"],
                        "catch_exceptions": required_kwargs["catch_exceptions"],
                    }
                },
                runtime_env=required_kwargs["runtime_env"],
            )

        @pytest.mark.parametrize(
            "kwargs,expected",
            [
                ({}, {}),
                ({"num_cpus": None}, {}),
                ({"num_cpus": 2}, {"num_cpus": 2}),
                ({"num_gpus": 1}, {"num_gpus": 1}),
                ({"memory": 1}, {"memory": 1}),
            ],
        )
        def test_resources(
            self, client: RayClient, remote_fn, required_kwargs, kwargs, expected
        ):
            print(kwargs)
            client.add_options(remote_fn, **required_kwargs, **kwargs)
            remote_fn.options.assert_called_with(
                _metadata={
                    "workflow.io/options": {
                        "task_id": required_kwargs["name"],
                        "metadata": required_kwargs["metadata"],
                        "catch_exceptions": required_kwargs["catch_exceptions"],
                    }
                },
                runtime_env=required_kwargs["runtime_env"],
                **expected,
            )
